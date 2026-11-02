<?php

/*
 *
 *  ____            _        _   __  __ _                  __  __ ____
 * |  _ \ ___   ___| | _____| |_|  \/  (_)_ __   ___      |  \/  |  _ \
 * | |_) / _ \ / __| |/ / _ \ __| |\/| | | '_ \ / _ \_____| |\/| | |_) |
 * |  __/ (_) | (__|   <  __/ |_| |  | | | | | |  __/_____| |  | |  __/
 * |_|   \___/ \___|_|\_\___|\__|_|  |_|_|_| |_|\___|     |_|  |_|_|
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * @author PocketMine Team
 * @link http://www.pocketmine.net/
 *
 *
*/

declare(strict_types=1);

namespace pmmp\RconServer;

use pocketmine\snooze\SleeperHandlerEntry;
use pocketmine\thread\log\ThreadSafeLogger;
use pocketmine\thread\Thread;
use pocketmine\utils\Binary;
use function count;
use function ltrim;
use function microtime;
use function socket_accept;
use function socket_close;
use function socket_getpeername;
use function socket_last_error;
use function socket_read;
use function socket_select;
use function socket_set_block;
use function socket_set_nonblock;
use function socket_set_option;
use function socket_shutdown;
use function socket_strerror;
use function socket_write;
use function str_replace;
use function strlen;
use function substr;
use function trim;
use const SO_KEEPALIVE;
use const SO_LINGER;
use const SOCKET_ECONNRESET;
use const SOCKET_EWOULDBLOCK;
use const SOL_SOCKET;

class RconThread extends Thread{
	/** Request ID + packet type + two null terminators */
	private const MIN_PACKET_SIZE = 10;
	/** Seconds a slow client may hold up this thread while a response is written */
	private const WRITE_TIMEOUT = 5;
	/** Seconds an authenticated client must be idle before a new connection may take its slot */
	private const MIN_IDLE_TO_EVICT = 60;

	public string $cmd = "";
	public string $response = "";

	private bool $stop = false;

	public function __construct(
		private \Socket $socket,
		private string $password,
		private int $maxClients,
		private ThreadSafeLogger $logger,
		private \Socket $ipcSocket,
		private SleeperHandlerEntry $sleeperEntry
	){}

	/**
	 * Returns false if the client is gone or too slow to take the whole packet.
	 */
	private function writePacket(\Socket $client, int $requestID, int $packetType, string $payload) : bool{
		$pk = Binary::writeLInt($requestID)
			. Binary::writeLInt($packetType)
			. $payload
			. "\x00\x00"; //Terminate payload and packet
		$buffer = Binary::writeLInt(strlen($pk)) . $pk;
		$deadline = microtime(true) + self::WRITE_TIMEOUT;
		while(true){
			$written = @socket_write($client, $buffer);
			if($written === false){
				if(socket_last_error($client) !== SOCKET_EWOULDBLOCK){
					return false;
				}
				$written = 0;
			}
			$buffer = substr($buffer, $written);
			if($buffer === ""){
				return true;
			}
			$remaining = $deadline - microtime(true);
			if($remaining <= 0){
				return false;
			}
			$r = null;
			$w = [$client];
			$e = null;
			@socket_select($r, $w, $e, 0, (int) ($remaining * 1_000_000));
		}
	}

	/**
	 * Finds the client to drop for a new connection when all slots are taken. Clients which died without closing the
	 * connection never time out by themselves, so without this they would eventually lock everyone out.
	 *
	 * @param bool[]  $authenticated
	 * @param float[] $lastActivity
	 */
	private function findIdleClient(array $authenticated, array $lastActivity) : ?int{
		$best = null;
		$cutoff = microtime(true) - self::MIN_IDLE_TO_EVICT;
		foreach($authenticated as $id => $status){
			if($status && $lastActivity[$id] <= $cutoff && ($best === null || $lastActivity[$id] < $lastActivity[$best])){
				$best = $id;
			}
		}
		return $best;
	}

	private function readPacket(\Socket $client, ?int &$requestID, ?int &$packetType, ?string &$payload) : bool{
		$d = @socket_read($client, 4);

		@socket_getpeername($client, $ip, $port);
		if($d === false){
			$err = socket_last_error($client);
			if($err !== SOCKET_ECONNRESET){
				$this->logger->debug("Connection error with $ip $port: " . trim(socket_strerror($err)));
			}
			return false;
		}
		if(strlen($d) !== 4){
			if($d !== ""){ //empty data is returned on disconnection
				$this->logger->debug("Truncated packet from $ip $port (want 4 bytes, have " . strlen($d) . "), disconnecting");
			}
			return false;
		}
		$size = Binary::readLInt($d);
		if($size < self::MIN_PACKET_SIZE or $size > 65535){
			$this->logger->debug("Packet with invalid length header $size from $ip $port, disconnecting");
			return false;
		}
		$buf = @socket_read($client, $size);
		if($buf === false){
			$err = socket_last_error($client);
			if($err !== SOCKET_ECONNRESET){
				$this->logger->debug("Connection error with $ip $port: " . trim(socket_strerror($err)));
			}
			return false;
		}
		if(strlen($buf) !== $size){
			$this->logger->debug("Truncated packet from $ip $port (want $size bytes, have " . strlen($buf) . "), disconnecting");
			return false;
		}
		$requestID = Binary::readLInt(substr($buf, 0, 4));
		$packetType = Binary::readLInt(substr($buf, 4, 4));
		$payload = substr($buf, 8, -2); //Strip two null bytes
		return true;
	}

	public function close() : void{
		$this->stop = true;
	}

	protected function onRun() : void{
		/** @var \Socket[] $clients */
		$clients = [];
		/** @var bool[] $authenticated */
		$authenticated = [];
		/** @var float[] $timeouts */
		$timeouts = [];
		/** @var float[] $lastActivity */
		$lastActivity = [];

		/** @var int $nextClientId */
		$nextClientId = 0;

		$notifier = $this->sleeperEntry->createNotifier();

		while(!$this->stop){
			$r = $clients;
			$r["main"] = $this->socket; //this is ugly, but we need to be able to mass-select()
			$r["ipc"] = $this->ipcSocket;
			$w = null;
			$e = null;

			$disconnect = [];

			if(@socket_select($r, $w, $e, 5, 0) > 0){
				foreach($r as $id => $sock){
					if($sock === $this->socket){
						if(($client = @socket_accept($this->socket)) !== false){
							$full = count($clients) >= $this->maxClients;
							$evict = $full ? $this->findIdleClient($authenticated, $lastActivity) : null;
							if($full && $evict === null){
								@socket_close($client);
							}else{
								if($evict !== null){
									$disconnect[$evict] = $clients[$evict];
								}
								socket_set_nonblock($client);
								socket_set_option($client, SOL_SOCKET, SO_KEEPALIVE, 1);

								$id = $nextClientId++;
								$clients[$id] = $client;
								$authenticated[$id] = false;
								$timeouts[$id] = microtime(true) + 5;
								$lastActivity[$id] = microtime(true);
							}
						}
					}elseif($sock === $this->ipcSocket){
						//read dummy data
						socket_read($sock, 65535);
					}else{
						$p = $this->readPacket($sock, $requestID, $packetType, $payload);
						if($p === false){
							$disconnect[$id] = $sock;
							continue;
						}
						$lastActivity[$id] = microtime(true);

						switch($packetType){
							case 3: //Login
								if($authenticated[$id]){
									$disconnect[$id] = $sock;
									break;
								}
								@socket_getpeername($sock, $addr, $port);
								if($payload === $this->password){
									$this->logger->info("Successful Rcon connection from: /$addr:$port");
									if(!$this->writePacket($sock, $requestID, 2, "")){
										$disconnect[$id] = $sock;
										break;
									}
									$authenticated[$id] = true;
								}else{
									$disconnect[$id] = $sock;
									$this->writePacket($sock, -1, 2, "");
									$this->logger->info("Unsuccessful connection from: /$addr:$port (wrong password)");
								}
								break;
							case 2: //Command
								if(!$authenticated[$id]){
									$disconnect[$id] = $sock;
									break;
								}
								if($payload !== ""){
									$this->cmd = ltrim($payload);
									$this->synchronized(function() use ($notifier) : void{
										$notifier->wakeupSleeper();
										$this->wait();
									});
									if(!$this->writePacket($sock, $requestID, 0, str_replace("\n", "\r\n", trim($this->response)))){
										$disconnect[$id] = $sock;
									}
									$this->response = "";
									$this->cmd = "";
								}
								break;
						}
					}
				}
			}

			foreach($authenticated as $id => $status){
				if(!isset($disconnect[$id]) and !$authenticated[$id] and $timeouts[$id] < microtime(true)){ //Timeout
					$disconnect[$id] = $clients[$id];
				}
			}

			foreach($disconnect as $id => $client){
				$this->disconnectClient($client);
				unset($clients[$id], $authenticated[$id], $timeouts[$id], $lastActivity[$id]);
			}
		}

		foreach($clients as $client){
			$this->disconnectClient($client);
		}
	}

	private function disconnectClient(\Socket $client) : void{
		@socket_getpeername($client, $ip, $port);
		@socket_set_option($client, SOL_SOCKET, SO_LINGER, ["l_onoff" => 1, "l_linger" => 1]);
		@socket_shutdown($client, 2);
		@socket_set_block($client);
		@socket_read($client, 1);
		@socket_close($client);
		$this->logger->info("Disconnected client: /$ip:$port");
	}

	public function getThreadName() : string{
		return "RCON";
	}
}
