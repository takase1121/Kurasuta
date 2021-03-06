import { EventEmitter } from 'events';
import { Client, Util } from 'discord.js';
import { IPCEvents } from '../Util/Constants';
import { IPCResult } from '..';
import { IPCError } from '../Sharding/ShardClientUtil';

import fastRedis = require('redis-fast-driver');

export interface IPCRequest {
	op: number;
	d: string;
}

export class ClusterIPC extends EventEmitter {
	public clientSocket?: ClientSocket;
	public client: Client;
	public node: VezaClient;

	constructor(discordClient: Client, public id: number, public socket: object) {
		super();
		this.client = discordClient;
		this.node = new VezaClient(`Cluster ${this.id}`)
			.on('error', error => this.emit('error', error))
			.on('disconnect', client => this.emit('warn', `[IPC] Disconnected from ${client.name}`))
			.on('ready', client => this.emit('debug', `[IPC] Connected to: ${client.name}`))
			.on('message', this._message.bind(this));
	}

	public async broadcast(script: string | Function) {
		script = typeof script === 'function' ? `(${script})(this)` : script;
		const { success, d } = await this.server.send({ op: IPCEvents.BROADCAST, d: script }) as IPCResult;
		if (!success) throw Util.makeError(d as IPCError);
		return d as unknown[];
	}

	public async masterEval(script: string | Function) {
		script = typeof script === 'function' ? `(${script})(this)` : script;
		const { success, d } = await this.server.send({ op: IPCEvents.MASTEREVAL, d: script }) as IPCResult;
		if (!success) throw Util.makeError(d as IPCError);
		return d as unknown;
	}

	public async init() {
		this.clientSocket = await this.node.connectTo(String(this.socket));
	}

	public get server() {
		return this.clientSocket!;
	}

	private _eval(script: string): string {
		return (this.client as any)._eval(script);
	}

	private async _message(message: NodeMessage) {
		const { op, d } = message.data;
		if (op === IPCEvents.EVAL) {
			try {
				message.reply({ success: true, d: await this._eval(d) });
			} catch (error) {
				message.reply({ success: false, d: { name: error.name, message: error.message, stack: error.stack } });
			}
		}
	}
}
