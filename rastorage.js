/**
 *	Author: JCloudYu
 *	Create: 2018/11/27
**/
(()=>{
	"use strict";
	
	const {promises:fs} = require( 'fs' );
	const path = require( 'path' );
	const ExtPromise = require( 'jsboost/native/ext-promise' );


	const LOCK_TYPE = { NONE:0, READ:1, WRITE:2, EXCLUSIVE:3 };
	const DB_STATE = { OK: 0, CLOSING: 1, CLOSED: 2 };
	const THROTTLE_OP_TYPE	 = {
		GET:0, PUT:1, DEL:2, SET:3, CLOSE:4
	};
	
	const TRUNCATE_BOUNDARY	 = 1;
	const BLOCK_INIT_ID_SIZE = 1;
	const BLOCK_ID_SIZE		 = 4;
	const BLOCK_LENGTH_SIZE	 = 1;
	const BLOCK_CONTENT_SIZE = 255;
	const BLOCK_HEADER_SIZE	 = BLOCK_INIT_ID_SIZE + BLOCK_ID_SIZE + BLOCK_LENGTH_SIZE;
	const BLOCK_SIZE = BLOCK_HEADER_SIZE + BLOCK_CONTENT_SIZE;
	const UNUSED_BLOCK = Buffer.alloc(BLOCK_SIZE);

	const DEFAULT_SEGD_HEADER	= Buffer.from([0x00, 0x00, 0x00, 0x00]);
	const SEGD_HEADER_SIZE		= DEFAULT_SEGD_HEADER.length;
	const SEGD_ITEM_SIZE		= SEGD_HEADER_SIZE;
	const DEFAULT_BLST_HEADER	= Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00]);
	const BLST_HEADER_SIZE		= DEFAULT_BLST_HEADER.length;
	
	




	
	/**
	 * @type {WeakMap<RAStorage, RAStoragePrivates>}
	 * @private
	**/
	const _RAStorage = new WeakMap();
	class RAStorage {
		constructor(err=true) {
			if ( err ) {
				throw new ReferenceError( "RAStorage instance should be obtained bia RAStorage.initAtPath method!" );
			}
			
			/** @type {RAStoragePrivates} */
			const PROPS = {
				version: 0,
				keep_alive: setInterval(()=>{}, 86400000),
				throttle_queue: [],
				throttle_timeout: ___GEN_TIMEOUT(),
				segment_list: [],
				root: null,
				total_blocks: 0,
				blst_fd: null,
				segd_fd: null,
				state: DB_STATE.CLOSED,
				is_dirty: false
			};
			_RAStorage.set(this, PROPS);
			
			
			this._serializer = null;
			this._deserializer = null;
		}
		
		/**
		 * Get data from db
		 *
		 * @async
		 * @param {Number} id The block id to retrieve
		 * @returns {Promise<*>}
		**/
		get(id) {
			const {throttle_queue, throttle_timeout, state} = _RAStorage.get(this);
			
			if ( state !== DB_STATE.OK ) {
				return Promise.reject(new Error("Database has been closed!"));
			}
			else {
				const promise = ___PROMISE();
				throttle_queue.push({op:THROTTLE_OP_TYPE.GET, id, promise});
				throttle_timeout(___THROTTLE_TIMEOUT, 0, this);
				return promise.p;
			}
		}
		
		/**
		 * Write data to db
		 *
		 * @async
		 * @param {*} data
		 * @returns {Promise<Number>}
		**/
		put(data) {
			const {throttle_queue, throttle_timeout, state} = _RAStorage.get(this);
		
			if ( state !== DB_STATE.OK ) {
				return Promise.reject(new Error("Database has been closed!"));
			}
			else {
				data = this._serializer ? this._serializer(data) : data;
				data = ___OBTAIN_BUFFER(data);
				
				const promise = ___PROMISE();
				throttle_queue.push({op:THROTTLE_OP_TYPE.PUT, data, promise});
				throttle_timeout(___THROTTLE_TIMEOUT, 0, this);
				return promise.p;
			}
		}
		
		/**
		 * Write data to db
		 *
		 * @async
		 * @param {Number} id The block id to retrieve
		 * @param {*} data
		 * @returns {Promise}
		**/
		set(id, data) {
			const {throttle_queue, throttle_timeout, state} = _RAStorage.get(this);
		
			if ( state !== DB_STATE.OK ) {
				return Promise.reject(new Error("Database has been closed!"));
			}
			else {
				data = this._serializer ? this._serializer(data) : data;
				data = ___OBTAIN_BUFFER(data);
				
				const promise = ___PROMISE();
				throttle_queue.push({op:THROTTLE_OP_TYPE.SET, id, data, promise});
				throttle_timeout(___THROTTLE_TIMEOUT, 0, this);
				return promise.p;
			}
		}
		
		/**
		 * Remove data from db
		 *
		 * @async
		 * @param {Number} id The block id to retrieve
		 * @returns {Promise}
		**/
		del(id) {
			const {throttle_queue, throttle_timeout, state} = _RAStorage.get(this);
			
			if ( state !== DB_STATE.OK ) {
				return Promise.reject(new Error("Database has been closed!"));
			}
			else {
				const promise = ___PROMISE();
				throttle_queue.push({op:THROTTLE_OP_TYPE.DEL, id, promise});
				throttle_timeout(___THROTTLE_TIMEOUT, 0, this);
				
				return promise.p;
			}
		}
		
		/**
		 * Close database connection
		 *
		 * @async
		 * @returns {Promise}
		**/
		close() {
			const _PRIVATE = _RAStorage.get(this);
			const {throttle_queue, throttle_timeout} = _PRIVATE;
			
			if ( _PRIVATE.state !== DB_STATE.OK ) {
				if ( _PRIVATE.state === DB_STATE.CLOSING ) {
					return Promise.reject(new Error( "Storage is closing now!" ));
				}
				else {
					return Promise.resolve();
				}
			}
			else {
				const promise = ___PROMISE();
				_PRIVATE.state = DB_STATE.CLOSING;
				
				throttle_queue.push({op:THROTTLE_OP_TYPE.CLOSE, promise});
				throttle_timeout(___THROTTLE_TIMEOUT, 0, this);
				
				return promise.p;
			}
		}
		
		/**
		 * Initialize a database at storage
		 *
		 * @async
		 * @param {String} storage_dir
		 * @returns {Promise<RAStorage>}
		**/
		static async InitAtPath(storage_dir) {
			const STORAGE_ROOT_PATH = path.resolve(storage_dir);
			// region [ Check & create storage root path ]
			let item_stat = await fs.stat(STORAGE_ROOT_PATH).catch((e)=>{
				if ( e.code !== "ENOENT" ) {
					return Promise.reject(e);
				}
				
				return null;
			});
			if ( !item_stat ) {
				await fs.mkdir(STORAGE_ROOT_PATH, {recursive:true});
			}
			else
			if ( !item_stat.isDirectory() ) {
				throw new Error( `Target directory ${storage_dir} is not a directory!` );
			}
			// endregion
			
			
			
			const SEGMENT_DESCRIPTOR_PATH = `${STORAGE_ROOT_PATH}/storage.segd`;
			const STORAGE_DATA_CONTAINER = `${STORAGE_ROOT_PATH}/storage.blst`;
			// region [ Check & create storage content ]
			// Init segment descriptor
			item_stat = await fs.stat(SEGMENT_DESCRIPTOR_PATH).catch((e)=>{
				if ( e.code !== "ENOENT" ) {
					return Promise.reject(e);
				}
				
				return null;
			});
			if ( !item_stat ) {
				await fs.writeFile(SEGMENT_DESCRIPTOR_PATH, DEFAULT_SEGD_HEADER);
			}
			else
			if ( !item_stat.isFile() ) {
				throw new Error( `${SEGMENT_DESCRIPTOR_PATH} is not a valid segd file!` );
			}
			
			// Init content container
			item_stat = await fs.stat(STORAGE_DATA_CONTAINER).catch((e)=>{
				if ( e.code !== "ENOENT" ) {
					return Promise.reject(e);
				}
				
				return null;
			});
			if ( !item_stat ) {
				await fs.writeFile(STORAGE_DATA_CONTAINER, DEFAULT_BLST_HEADER);
			}
			else
			if ( !item_stat.isFile() ) {
				throw new Error( `${SEGMENT_DESCRIPTOR_PATH} is not a valid blst file!` );
			}
			// endregion
			
			
			
			const STORAGE  = new RAStorage(false);
			const _PRIVATE = _RAStorage.get(STORAGE);
			const DataBuffer = Buffer.alloc(Math.max(BLST_HEADER_SIZE, SEGD_HEADER_SIZE, SEGD_ITEM_SIZE));
			// region [ Load storage ]
			_PRIVATE.root = STORAGE_ROOT_PATH;
			const SEGD_FD = _PRIVATE.segd_fd = await fs.open(SEGMENT_DESCRIPTOR_PATH, 'r+');
			const BLST_FD = _PRIVATE.blst_fd = await fs.open(STORAGE_DATA_CONTAINER,  'r+');
			
			// Read content container's block sizes
			await BLST_FD.read(DataBuffer, 0, BLST_HEADER_SIZE, 0);
			_PRIVATE.version = DataBuffer.readUInt8(0);
			_PRIVATE.total_blocks = DataBuffer.readUInt32LE(1);
			
			// Read segmentation descriptor size
			await SEGD_FD.read(DataBuffer, 0, SEGD_HEADER_SIZE, 0);
			let segments_remain = DataBuffer.readUInt32LE(0);
			let fPointer = SEGD_HEADER_SIZE, segment_buffer = [];
			while(segments_remain-->0) {
				await SEGD_FD.read(DataBuffer, 0, SEGD_ITEM_SIZE, fPointer);
				segment_buffer.push(DataBuffer.readUInt32LE(0));
				fPointer += SEGD_ITEM_SIZE;
			}
			_PRIVATE.segment_list = segment_buffer.sort(___SEGD_CMP);
			// endregion
			
			
			_PRIVATE.state = DB_STATE.OK;
			return STORAGE;
		}
	}
	module.exports = {RAStorage};
	
	
	
	
	
	/**
	 * @param {ArrayBuffer|Buffer|Uint32Array|Uint16Array|Uint8Array|Int8Array|Int32Array|Int16Array|Float32Array|Float64Array|DataView} data
	 * @returns {Buffer}
	 * @private
	**/
	function ___OBTAIN_BUFFER(data) {
		if ( data instanceof ArrayBuffer ) {
			return Buffer.from(data);
		}
		
		if ( Buffer.isBuffer(data) ) {
			return data;
		}
		
		if ( ArrayBuffer.isView(data) ) {
			return Buffer.from(data.buffer);
		}
	}
	/**
	 * @returns {StatePromise}
	 * @private
	**/
	function ___PROMISE() {
		/** @type Function */
		let res, rej;
		const promise = new Promise((_res, _rej)=>{res=_res; rej=_rej;});
		return {p:promise, res, rej};
	}
	/**
	 * @returns {function(function, number, ...[*])}
	 * @private
	**/
	function ___GEN_TIMEOUT() {
		let cb_buff	 = null;
		let hTimeout = null;
		return (cb, delay, ...args)=>{
			cb_buff={cb, delay, args};
			
			if ( !hTimeout ) {
				___DO_TIMEOUT();
			}
		};
		
		function ___DO_TIMEOUT() {
			if ( !cb_buff ) return;
			
			const {cb:callback, delay:cb_delay, args:cb_args} = cb_buff;
			cb_buff  = null;
			hTimeout = setTimeout(()=>{
				Promise.resolve(callback(...cb_args))
				.then(()=>{
					hTimeout = null;
					___DO_TIMEOUT();
				})
				.catch((e)=>{
					hTimeout = null; throw e;
				});
			}, cb_delay);
		}
	}
	/**
	 * @param {Number} a
	 * @param {Number} b
	 * @returns {Number}
	 * @private
	**/
	function ___SEGD_CMP(a, b) {
		return (a>b?1:(a<b?-1:0));
	}
	/**
	 * @param {RAStorage} inst
	 * @private
	**/
	async function ___THROTTLE_TIMEOUT(inst) {
		const _PRIVATE = _RAStorage.get(inst);
		const {throttle_queue, throttle_timeout} = _PRIVATE;
		if ( throttle_queue.length <= 0 ) return;
	
		
		
		// Filter out the blocked operations from safe ops
		const locker = new Map();
		const push_back = [], op_queue = [];
		while( throttle_queue.length > 0 ) {
			const operation = throttle_queue.shift();
			if ( operation.op === THROTTLE_OP_TYPE.PUT ) {
				op_queue.push(operation);
			}
			else
			if ( operation.op === THROTTLE_OP_TYPE.CLOSE ) {
				op_queue.push(operation);
			}
			else
			if ( operation.op === THROTTLE_OP_TYPE.GET ) {
				const lock = ___GET_LOCK(locker, operation.id);
				if ( lock._t > LOCK_TYPE.READ ) {
					push_back.push(operation);
				}
				else {
					lock._t = LOCK_TYPE.READ;
					op_queue.push(operation);
				}
			}
			else
			if ( operation.op === THROTTLE_OP_TYPE.DEL || operation.op === THROTTLE_OP_TYPE.SET ) {
				const lock = ___GET_LOCK(locker, operation.id);
				if ( lock._t !== LOCK_TYPE.NONE ) {
					if ( lock._t === LOCK_TYPE.READ ) {
						lock._t = LOCK_TYPE.EXCLUSIVE;
					}
					push_back.push(operation);
				}
				else {
					lock._t = LOCK_TYPE.WRITE;
					op_queue.push(operation);
				}
			}
		}
		
		// Push the blocked operations back into throttle queue
		throttle_queue.splice(0, 0, ...push_back);
		
		// Process operations and make sure
		const promises = [], op_map = [];
		for( let operation of op_queue ) {
			let promise = null;
			switch(operation.op) {
				case THROTTLE_OP_TYPE.GET:
					promise = ___OPERATION_GET(inst, operation);
					op_map.push(operation);
					break;
				case THROTTLE_OP_TYPE.PUT:
					promise = ___OPERATION_PUT(inst, operation);
					op_map.push(operation);
					break;
				case THROTTLE_OP_TYPE.SET:
					promise = ___OPERATION_SET(inst, operation);
					op_map.push(operation);
					break;
				case THROTTLE_OP_TYPE.DEL:
					promise = ___OPERATION_DEL(inst, operation);
					op_map.push(operation);
					break;
				case THROTTLE_OP_TYPE.CLOSE:
					if ( throttle_queue.length <= 0 && op_queue.length === 1 ) {
						promise = ___OPERATION_CLOSE(inst, operation);
						op_map.push(operation);
					}
					else {
						throttle_queue.push(operation);
					}
					break;
			}
			
			if ( promise ) {
				promises.push(promise);
			}
		}
		
		
		
		// Await for all the operations are done and respond operations' promises
		const exec_results = await ExtPromise.WaitAll(promises).catch(ret=>ret);
		for(let i=0; i<exec_results.length; i++) {
			const status = exec_results[i];
			if ( status.resolved ) {
				op_map[i].promise.res(status.result);
			}
			else {
				op_map[i].promise.rej(status.result);
			}
		}
		
		
		
		// Update total blocks and other segment list...
		if ( _PRIVATE.is_dirty ) {
			await ___DIRTY_WORK(inst);
			_PRIVATE.is_dirty = false;
		}
		
		
		
		// Keep resolving throttle if there still are operations in the throttle queue
		if ( throttle_queue.length > 0 ) {
			throttle_timeout(___THROTTLE_TIMEOUT, 0, inst);
		}
	}
	
	
	
	
	/**
	 * @param {RAStorage} inst
	 * @param {RAStorageOperation} operation
	 * @returns {Promise}
	 * @private
	**/
	async function ___OPERATION_GET(inst, operation) {
		const {id} = operation;
		
		let data_buff = [], buff_size = 0;
		
		try {
			// NOTE: Read initial block
			let block = await ___READ_BLOCK(inst, id).catch(()=>{ return null; });
			if ( !block || !block.root ) {
				return undefined;
			}
			
			// NOTE: Read all blocks
			data_buff.push(block.content);
			buff_size += block.contentLength;
			while( block.next !== 0) {
				block = await ___READ_BLOCK(inst, block.next);
				data_buff.push(block.content);
				buff_size += block.contentLength;
			}
			
			// NOTE: Merge contents
			const resultBuff = new Uint8Array(buff_size);
			for(let i=0, anchor=0; i<data_buff.length; i++) {
				const content = data_buff[i];
				resultBuff.set(content, anchor);
				anchor += content.byteLength;
			}
			
			// NOTE: Resolve the original promise
			return !inst._deserializer?resultBuff.buffer:inst._deserializer(resultBuff.buffer);
		}
		catch(e) {
			throw e;
		}
	}
	/**
	 * @param {RAStorage} inst
	 * @param {RAStorageOperation} operation
	 * @returns {Promise}
	 * @private
	**/
	async function ___OPERATION_PUT(inst, operation) {
		const {data} = operation;
		const NUM_BLOCKS = (data.length <= 0) ? 1 : Math.ceil(data.length/BLOCK_CONTENT_SIZE);
		const BLOCK_IDS	 = ___ALLOCATE_BLOCKS(inst, NUM_BLOCKS);

		let anchor = 0, promises = [], initId = BLOCK_IDS[0];
		while(BLOCK_IDS.length > 0) {
			let blockId = BLOCK_IDS.shift();
			let buff = data.slice(anchor, anchor + BLOCK_CONTENT_SIZE);
			promises.push(___WRITE_BLOCK(inst, blockId, buff, BLOCK_IDS[0]||0, anchor===0));
			anchor += buff.length;
		}
		
		try {
			await ExtPromise.WaitAll(promises);
			return initId;
		}
		catch(e) {
			let error = new Error( "Cannot put contents into blocks!" );
			error.detail = [];
			for( let result of e ) {
				if (!result.resolved) {
					error.detail.push(result.result);
				}
			}
			
			throw error;
		}
	}
	/**
	 * @param {RAStorage} inst
	 * @param {RAStorageOperation} operation
	 * @returns {Promise}
	 * @private
	**/
	async function ___OPERATION_SET(inst, operation) {
		const {id, data} = operation;
		
		
		// NOTE: Read initial block
		let remaining_blocks = (data.length <= 0) ? 1 : Math.ceil(data.length/BLOCK_CONTENT_SIZE);
		let block = await ___READ_BLOCK(inst, id);
		if ( !block.root ) {
			throw new RangeError(`Target file block #${id} is not an initial block!`);
		}
		
		
		let anchor = 0, blockId = id;
		while(remaining_blocks>0 && block.next!==0) {
			remaining_blocks--;
			
			let buff = data.slice(anchor, anchor+BLOCK_CONTENT_SIZE);
			await ___WRITE_BLOCK(inst, blockId, buff, remaining_blocks === 0 ? 0 : block.next, anchor===0);
			block = await ___READ_BLOCK(inst, blockId=block.next);
			anchor += buff.length;
		}
		
		if ( remaining_blocks > 0 ) {
			if ( remaining_blocks === 1 ) {
				let buff = data.slice(anchor, anchor+BLOCK_CONTENT_SIZE);
				await ___WRITE_BLOCK(inst, blockId, buff, 0, anchor===0);
				return undefined;
			}
			else {
				const BLOCK_IDS = ___ALLOCATE_BLOCKS(inst, remaining_blocks - 1);
				BLOCK_IDS.unshift(blockId);
				
				let promises = [];
				while(BLOCK_IDS.length > 0) {
					blockId = BLOCK_IDS.shift();
					let buff = data.slice(anchor, anchor+BLOCK_CONTENT_SIZE);
					promises.push(___WRITE_BLOCK(inst, blockId, buff, BLOCK_IDS[0]||0, anchor===0));
					anchor += buff.length;
				}
				
				try {
					await ExtPromise.WaitAll(promises);
					return undefined;
				}
				catch(e) {
					let error = new Error( "Cannot put contents into blocks!" );
					error.detail = [];
					for( let result of e ) {
						if (!result.resolved) {
							error.detail.push(result.result);
						}
					}
					
					throw error;
				}
			}
		}
		else {
			while(block.next!==0) {
				await ___FREE_BLOCK(inst, blockId);
				block = await ___READ_BLOCK(inst, blockId=block.next);
			}
			
			await ___FREE_BLOCK(inst, blockId);
			return undefined;
		}
	}
	/**
	 * @param {RAStorage} inst
	 * @param {RAStorageOperation} operation
	 * @returns {Promise}
	 * @private
	**/
	async function ___OPERATION_DEL(inst, operation) {
		const {id} = operation;
		
		let block = await ___READ_BLOCK(inst, id);
		if ( !block.root ) {
			throw new RangeError(`Target file block #${id} is not an initial block!`);
		}
		
		let blockId = id;
		while(block.next!==0) {
			await ___FREE_BLOCK(inst, blockId);
			block = await ___READ_BLOCK(inst, blockId=block.next);
		}
		
		await ___FREE_BLOCK(inst, blockId);
		return undefined;
	}
	/**
	 * @param {RAStorage} inst
	 * @param {RAStorageOperation} operation
	 * @returns {Promise}
	 * @private
	**/
	async function ___OPERATION_CLOSE(inst, operation) {
		const _PRIVATE = _RAStorage.get(inst);
		
		try {
			await ExtPromise.WaitAll([_PRIVATE.blst_fd.close(), _PRIVATE.segd_fd.close()]);
			clearInterval(_PRIVATE.keep_alive);
			_PRIVATE.state = DB_STATE.CLOSED;
			_PRIVATE.keep_alive = null;
			return undefined;
		}
		catch(e) {
			const error= new Error( "Cannot close database!" );
			error.detail = [];
			for(let res of e) {
				if ( !res.resolved ) {
					error.detail.push(res.result);
				}
			}
			
			throw error;
		}
	}
	
	
	/**
	 *
	 * @async
	 * @param {RAStorage} inst
	 * @private
	**/
	async function ___DIRTY_WORK(inst) {
		const _PRIVATE = _RAStorage.get(inst);
		const {segd_fd, blst_fd, total_blocks, segment_list} = _PRIVATE;
		
		// NOTE: Do truncate if necessary
		if ( segment_list.length > 0 ) {
			const list = segment_list.slice().sort(___SEGD_CMP);
			if ( list[list.length-1] === total_blocks ) {
				let last = list.pop();
				let truncate_pos = [last];
				while( list.length>0 && list[list.length-1]===(last-1) ) {
					truncate_pos.unshift(last=list.pop());
				}
				
				if ( truncate_pos.length >= TRUNCATE_BOUNDARY ) {
					await blst_fd.truncate(BLST_HEADER_SIZE + (truncate_pos[0]-1) * BLOCK_SIZE);
					_PRIVATE.total_blocks -= truncate_pos.length;
					segment_list.splice(0, segment_list.length, ...list);
				}
			}
		}
		
		
		
		
		
		const blst_header = Buffer.alloc(BLST_HEADER_SIZE);
		blst_header.writeUInt8(0x01, 0);
		blst_header.writeUInt32LE(_PRIVATE.total_blocks, 1);
		await blst_fd.write(blst_header, 0, BLST_HEADER_SIZE, 0);
		
		
		
		
		const segd_header = Buffer.alloc(SEGD_HEADER_SIZE);
		segd_header.writeUInt32LE(segment_list.length, 0);
		await segd_fd.write(segd_header, 0, SEGD_HEADER_SIZE, 0);
		
		const segd_item = Buffer.alloc(SEGD_ITEM_SIZE);
		for(let i=0; i<segment_list.length; i++) {
			segd_item.writeUInt32LE(segment_list[i], 0);
			await segd_fd.write(segd_item, 0, SEGD_ITEM_SIZE, SEGD_HEADER_SIZE + i*SEGD_ITEM_SIZE);
		}
	}
	/**
	 * @async
	 * @param {RAStorage} inst
	 * @param {Number} blockId
	 * @returns DataBlock
	 * @private
	**/
	async function ___READ_BLOCK(inst, blockId) {
		const {total_blocks, blst_fd} = _RAStorage.get(inst);
		if ( blockId <= 0 || blockId > total_blocks ) {
			throw new RangeError( `Requested block #${blockId} is out of range!` );
		}
		
		let buff = Buffer.alloc(BLOCK_SIZE);
		await blst_fd.read(buff, 0, BLOCK_SIZE, BLST_HEADER_SIZE + (blockId-1)*BLOCK_SIZE);
		
		let
		root = buff[0],
		next = buff.readUInt32LE(1),
		contentLength = buff.readUInt8(5),
		content = buff.slice(6, 6+contentLength);
		
		
		
		return { root, next, contentLength, content };
	}
	/**
	 * @async
	 * @param {RAStorage} inst
	 * @param {Number} blockId
	 * @param {Buffer} content
	 * @param {Number} [next=0]
	 * @param {Boolean} [is_root=false]
	 * @private
	**/
	async function ___WRITE_BLOCK(inst, blockId, content, next=0, is_root=false) {
		const {blst_fd} = _RAStorage.get(inst);
		if ( blockId <= 0 ) {
			throw new RangeError( `Requested block #${blockId} is out of range!` );
		}
		
		if ( content.length > 255 ) {
			throw new RangeError( `Block content size should not be larger than 255!` );
		}
		
		
		
		let buff = Buffer.alloc(BLOCK_SIZE);
		buff.writeUInt8(is_root?0xFF:0x00, 0);
		buff.writeUInt32LE(next, 1);
		buff.writeUInt8(content.length, 5);
		content.copy(buff, 6);
		
		await blst_fd.write(buff, 0, BLOCK_SIZE, BLST_HEADER_SIZE + (blockId-1)*BLOCK_SIZE);
	}
	/**
	 * @async
	 * @param {RAStorage} inst
	 * @param {Number} blockId
	 * @private
	**/
	async function ___FREE_BLOCK(inst, blockId) {
		const _PRIVATE = _RAStorage.get(inst);
		const {blst_fd, segment_list, total_blocks} = _PRIVATE;
		if ( blockId <= 0 || blockId > total_blocks ) {
			throw new RangeError( `Requested block #${blockId} is out of range!` );
		}
		
		await blst_fd.write(UNUSED_BLOCK, 0, BLOCK_SIZE, BLST_HEADER_SIZE + (blockId-1)*BLOCK_SIZE);
		segment_list.push(blockId);
		_PRIVATE.is_dirty = _PRIVATE.is_dirty || true;
	}
	/**
	 * @param {RAStorage} inst
	 * @param {Number} num_blocks
	 * @returns {Number[]}
	 * @private
	**/
	function ___ALLOCATE_BLOCKS(inst, num_blocks) {
		const _PRIVATE = _RAStorage.get(inst);
		const {segment_list} = _PRIVATE;
		
		let remaining_segments = segment_list.sort(___SEGD_CMP);
		let selected = remaining_segments.splice(0, num_blocks);
		_PRIVATE.segment_list = remaining_segments;
		
		
		
		let num_create = num_blocks - selected.length;
		for(let i=0; i<num_create; i++) {
			selected.push(++_PRIVATE.total_blocks);
		}
		if ( num_create > 0 ) {
			_PRIVATE.is_dirty = _PRIVATE.is_dirty || true;
		}
		
		return selected;
	}
	/**
	 * @param {Map<String, {_t:Number}>} locker
	 * @param {Number} id
	 * @returns {{_t:Number}}
	 * @private
	 */
	function ___GET_LOCK(locker, id) {
		const bId = `#${id}`;
		let lock = locker.get(bId);
		if ( !lock ) {
			locker.set(bId, lock={_t:LOCK_TYPE.NONE});
		}
		
		return lock;
	}
	
	
	
	
	
	
	// region [ Virtual class Definitions for JSDoc ]
	/**
	 * @class StatePromise
	 * @property {Promise} StatePromise.p
	 * @property {Function} StatePromise.res
	 * @property {Function} StatePromise.rej
	 * @private
	**/
	
	/**
	 * @class RAStoragePrivates
	 * @property {Array} RAStoragePrivates.throttle_queue
	 * @property {function(Function, Number, ...[*])} RAStoragePrivates.throttle_timeout
	 * @property {Number[]} RAStoragePrivates.segment_list
	 * @property {String|null} RAStoragePrivates.root
	 * @property {Number} RAStoragePrivates.total_blocks
	 * @property {*} RAStoragePrivates.blst_fd
	 * @property {*} RAStoragePrivates.segd_fd
	 * @property {Number} RAStoragePrivates.state
	 * @property {Boolean} RAStoragePrivates.is_dirty
	 * @private
	**/
	
	/**
	 * @class RAStorageOperation
	 * @property {Number} RStorageOperation.op
	 * @property {Number} [RStorageOperation.id]
	 * @property {Buffer} [RStorageOperation.data]
	 * @property {StatePromise} RStorageOperation.promise
	 * @private
	**/
	
	/**
	 * @class DataBlock
	 * @property {Number} DataBlock.root
	 * @property {Number} DataBlock.next
	 * @property {Number} DataBlock.contentLength
	 * @property {Uint8Array|null} DataBlock.content
	 * @private
	**/
	// endregion
})();
