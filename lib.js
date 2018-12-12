/**
 *	Author: JCloudYu
 *	Create: 2018/12/13
**/
(()=>{
	"use strict";
	
	const _ThrottledQueue = new WeakMap();
	class ThrottledQueue {
		constructor() {
			_ThrottledQueue.set(this, {
				_timeout: ThrottleTimeout(),
				_queue: []
			});
			
			this.consumer = null;
		}
		
		/** @type {Number} */
		get length() {
			return _ThrottledQueue.get(this)._queue.length;
		}
		
		/**
		 * Create
		 * @param {*} info
		 * @return Promise<*>;
		**/
		push(info=null) {
			const {_queue, _timeout} = _ThrottledQueue.get(this);
			const promise = PassivePromise();
			const item = {info, ctrl:promise};
			_queue.push(item);
			
			_timeout(___CONSUME_QUEUE, 0, this);
			return promise.promise;
		}
		
		/**
		 * Create a ThrottledQueue object with default consumer api
		 *
		 * @param {function(*[]):Promise<Boolean>} consumer
		 * @returns {ThrottledQueue}
		**/
		static CreateQueueWithConsumer(consumer) {
			const queue = new ThrottledQueue();
			queue.consumer = consumer;
			return queue;
		}
	}
	
	module.exports = {
		ThrottleTimeout,
		ThrottledQueue,
		PassivePromise,
		PromiseWaitAll
	};
	
	
	function ThrottleTimeout() {
		let _specialId	= Date.now();
		let _scheduled	= null;
		let _executing	= false;
		let _hTimeout	= null;
		
		const timeout_cb = (cb, delay=0, ...args)=>{
			_scheduled = {cb, delay, args};
			
			if ( _executing ) return;
			if ( _hTimeout ) {
				clearTimeout(_hTimeout);
				_hTimeout = null;
			}
			__DO_TIMEOUT();
		};
		timeout_cb.clear=()=>{
			_scheduled = null;
			if ( _hTimeout ) {
				clearTimeout(_hTimeout);
				_hTimeout = null;
			}
		};
		return timeout_cb;
		
		
		
		function __DO_TIMEOUT() {
			if ( !_scheduled ) return;
		
			let {cb, delay, args} = _scheduled;
			_hTimeout = setTimeout(()=>{
				_executing = true;
				
				Promise.resolve(cb(...args))
				.then(
					()=>{
						_executing = false;
						_hTimeout = null;
						
						__DO_TIMEOUT();
					},
					(e)=>{
						_executing	= false;
						_hTimeout	= null;
						_scheduled	= null;
						
						throw e;
					}
				);
			}, delay);
			_scheduled = null;
		}
	}
	function PassivePromise() {
		let resolve, reject;
		const promise = new Promise((_resolve, _reject)=>{resolve=_resolve; reject=_reject;});
		return {promise, resolve, reject}
	}
	function PromiseWaitAll(promise_queue=[]) {
		if ( !Array.isArray(promise_queue) ){
			promise_queue = [promise_queue];
		}
		
		if( promise_queue.length === 0 ){
			return Promise.resolve([]);
		}
		
		return new Promise((resolve, reject) =>{
			let result_queue=[], ready_count=0, resolved = true;
			for(let idx=0; idx<promise_queue.length; idx++) {
				let item = {resolved:true, seq:idx, result:null};
				
				result_queue.push(item);
				Promise.resolve(promise_queue[idx]).then(
					(result)=>{
						resolved = (item.resolved = true) && resolved;
						item.result = result;
					},
					(error)=>{
						resolved = (item.resolved = false) && resolved;
						item.result = error;
					}
				).then(()=>{
					ready_count++;
					
					if ( promise_queue.length === ready_count ) {
						(resolved?resolve:reject)(result_queue);
					}
				});
			}
		});
	}
	
	
	
	
	
	
	async function ___CONSUME_QUEUE(inst) {
		if ( typeof inst.consumer !== "function" ) return;
		
		
		
		const {_queue, _timeout} = _ThrottledQueue.get(inst);
		let should_continue = await inst.consumer(_queue);
		if ( should_continue === false ) return;
		if ( _queue.length <= 0 ) return;
	
		_timeout(___CONSUME_QUEUE, 0 , inst);
	}
})();
