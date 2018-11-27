/**
 *	Author: JCloudYu
 *	Create: 2018/11/27
**/
(()=>{
	"use strict";



	const fs 	= require( 'fs' );
	const path 	= require( 'path' );



	const _RAStorage = new WeakMap();

	const USELESS_SEGMENT_LENGTH 	= 4;
	const DATA_ID_LENGTH 			= 4;
	const DATA_SPACE_LENGTH 		= 1;
	const DATA_HEADER_LENGTH 		= DATA_ID_LENGTH + DATA_SPACE_LENGTH;
	const DATA_BODY_LENGTH 			= 255;
	const DATA_ALL_LENGTH 			= DATA_ID_LENGTH + DATA_SPACE_LENGTH + DATA_BODY_LENGTH;

	
	class RAStorage {
		constructor() {
			const PROPS = {};
			_RAStorage.set( this, PROPS );
		}
		async put(data) {
			const {rastorage_fd, segd, segd_fd} = _RAStorage.get(this);

			if( data instanceof ArrayBuffer )
				data = Buffer.from( data );
			if( !(data instanceof Buffer) ) throw new Error( `Data type should be Buffer or ArrayBuffer!` );


			const original_pieces = ___SPLIT_DATA( data );
			const pieces = [];

			for( const fbId of segd.frags ) {
				const piece = original_pieces.shift();
				if( !piece ) break;

				pieces.push( { posId: fbId, data: piece } );
			}

			for( const piece of original_pieces ) {
				pieces.push( { posId: segd.block_amount, data: piece } );
				segd.block_amount ++;
			}

			let prev_piece 	= pieces.shift();
			let firstId 	= prev_piece.posId;
			for( const piece of pieces ) {
				prev_piece.data.writeUInt32LE( piece.posId, 0 );
				await ___PROMISEFY( fs.write, fs, rastorage_fd, prev_piece.data, 0, prev_piece.data.length, prev_piece.posId * DATA_ALL_LENGTH );
				prev_piece = pieces;
			}
			await ___PROMISEFY( fs.write, fs, rastorage_fd, prev_piece.data, 0, prev_piece.data.length, prev_piece.posId * DATA_ALL_LENGTH );
			// TODO: Update segd file

			return firstId;
		}
		async overwrite(id, data) {
			await this.del( id );
			return await this.put( data );
		}
		async get(id) {
			const { rastorage_fd } = _RAStorage.get(this);
			const buff = Buffer.alloc( DATA_ALL_LENGTH );
			let nextId = null, result = undefined;
			id -= 1;

			try {
				await ___PROMISEFY( fs.read, fs, rastorage_fd, buff, 0, DATA_ALL_LENGTH, id * DATA_ALL_LENGTH );

				nextId = buff.readUInt32LE( 0 );
				const data_length = buff.readUInt8( DATA_ID_LENGTH );
				if( !data_length ) return result;

				result = Buffer.concat( [buff.slice( DATA_HEADER_LENGTH, DATA_ALL_LENGTH )] );

				while( nextId ) {
					await ___PROMISEFY( fs.read, fs, rastorage_fd, buff, 0, DATA_ALL_LENGTH, nextId * DATA_ALL_LENGTH );
					result = Buffer.concat( [ result, buff.slice( DATA_HEADER_LENGTH, DATA_ALL_LENGTH ) ] );
					nextId = buff.readUInt32LE( 0 );
				}

				result = result.buffer;
			}
			catch(e) {
				throw new Error( `Cannot get data! (${e})` );
			}

			return result;
		}
		async del(id) {
			const { rastorage_fd, segd, segd_fd } = _RAStorage.get(this);
			const header = Buffer.alloc( DATA_HEADER_LENGTH );

			header.writeUInt32LE( 0, 0 );
			header.writeUInt8( 0, DATA_ID_LENGTH );

			try {
				await ___PROMISEFY( fs.write, fs, rastorage_fd, 0, header, 0, header.length, (id - 1)* DATA_ALL_LENGTH );
				segd.useless_block_amount ++;
				segd.frags.push( id );
				// TODO: Update segd file ...
			}
			catch(e) {
				throw new Error( `Cannot delete data! ${e}` );
			}
		}
		
		
		
		static async initAtPath(dir) {
			const STORAGE_DIR 	= path.resolve( dir );
			const STORAGE 		= new RAStorage();
			const PROPS 		= _RAStorage.get( STORAGE );

			PROPS.segd_path 		= `${STORAGE_DIR}/useless.segd`;
			PROPS.rastorage_path 	= `${STORAGE_DIR}/rastorage.jlst`;

			await ___CREATE_DIR( dir );

			try {
				[PROPS.segd_fd] = await ___PROMISEFY( fs.open, fs, PROPS.segd_path, "r+" );
			}
			catch(e) {
				try {
					[PROPS.segd_fd] 	= await ___WRITE_USELESS_SEGD( PROPS.segd_path );
					[PROPS.segd] 		= await ___READ_SEGD( PROPS.segd_fd );
				}
				catch(e) {
					throw new Error( `Cannot access useless segments! (${e})` );
				}
			}

			try {
				[PROPS.rastorage_fd] = await ___PROMISEFY( fs.open, fs, PROPS.rastorage_path, "r+" );
			}
			catch(e) {
				try {
					[PROPS.rastorage_fd] = await ___OPEN_NEW_FILE( PROPS.rastorage_path );
				}
				catch(e) {
					throw new Error( `Cannot access rastorage! (${PROPS.rastorage_path})` )
				}
			}


			return STORAGE;
		}
	}
	
	module.exports = RAStorage;


	function ___SPLIT_DATA(data) {
		if( data instanceof ArrayBuffer )
			data = Buffer.from( data );
		if( !(data instanceof Buffer) ) throw new Error( `Data type should be Buffer or ArrayBuffer!` );

		const result = [];
		const split_number = Math.ceil( data.length / DATA_BODY_LENGTH );
		let curPos = 0;
		for( let i=0; i<split_number; i++ ) {
			const slice_raw 	= data.slice( curPos, curPos + DATA_BODY_LENGTH );
			const header_raw 	= ___FILL_DATA_HEADER( 0, slice_raw.length );

			curPos += DATA_BODY_LENGTH;
			result.push( Buffer.concat( [ header_raw, slice_raw ] ) );
		}

		return result;
	}

	function ___FILL_DATA_HEADER( blockId, spaceLen ) {
		blockId 	= blockId | 0;
		spaceLen 	= spaceLen | 0;

		const raw_id 	= Buffer.alloc( DATA_ID_LENGTH );
		const raw_space = Buffer.alloc( DATA_SPACE_LENGTH );

		raw_id.writeUInt32LE( blockId, 0 );
		raw_space.writeUInt8( spaceLen, 0 );

		return Buffer.concat( [ raw_id, raw_space ] );
	}

	async function ___READ_SEGD(segd_fd) {
		let rLen, segd_pos = 0, buff = Buffer.alloc( USELESS_SEGMENT_LENGTH );

		[rLen] = await ___PROMISEFY( fs.read, fs, segd_fd, buff, 0, USELESS_SEGMENT_LENGTH, segd_pos );
		if( rLen !== USELESS_SEGMENT_LENGTH ) throw new Error( `Cannot read blocks amount information!` );
		const block_amount = buff.readUInt32LE( 0 );
		segd_pos += USELESS_SEGMENT_LENGTH;

		[rLen] = await ___PROMISEFY( fs.read, fs, segd_fd, buff, 0, USELESS_SEGMENT_LENGTH, segd_pos );
		if( rLen !== USELESS_SEGMENT_LENGTH ) throw new Error( `Cannot read useless blocks amount information!` );
		const useless_block_amount = buff.readUInt32LE( 0 );
		segd_pos += USELESS_SEGMENT_LENGTH;


		const segd 	= {};
		const frags = [];
		while( segd_pos < ( useless_block_amount + 2 ) * USELESS_SEGMENT_LENGTH )
		{
			[rLen] = await ___PROMISEFY( fs.read, fs, segd_fd, buff, 0, USELESS_SEGMENT_LENGTH, segd_pos );
			if( rLen !== USELESS_SEGMENT_LENGTH ) throw new Error( `Insufficient data in useless segmentation!` );

			frags.push( buff.readUInt32LE( 0 ) );
			segd_pos += USELESS_SEGMENT_LENGTH;
		}



		segd.block_amount 			= block_amount;
		segd.useless_block_amount 	= useless_block_amount;
		segd.frags 					= frags;

		return segd;
	}

	async function ___OPEN_NEW_FILE(path) {
		const [fd] = await ___PROMISEFY( fs.open, fs, path, "a+" );
		await ___PROMISEFY( fs.close, fs, fd );
		return await ___PROMISEFY( fs.open, fs, path, "r+" );
	}

	async function ___WRITE_USELESS_SEGD(path) {
		let segd = Buffer.alloc( USELESS_SEGMENT_LENGTH );
		segd.writeUInt32LE( 0, 0 ); // How many blocks
		segd.writeUInt32LE( 0, 4 ); // How many free blocks
		await ___PROMISEFY( fs.writeFile, fs, path, segd );
		return await ___PROMISEFY( fs.open, fs, path, "r+" );
	}

	async function ___CREATE_DIR(dir) {
		try {
			await ___PROMISEFY( fs.access, fs, dir );
		}
		catch(e) {
			try {
				await ___PROMISEFY( fs.mkdir, fs, dir, { recursive: true } );
			}
			catch(e) {
				throw new Error( `Cannot create the rastorage directory! (${e})` );
			}
		}
	}

	function ___PROMISEFY(func, thisArg=null, ...args) {
		return new Promise((resolve, reject)=>{
			func.call(thisArg, ...args, (err, ...results)=> {
				if( err ) return reject(err);
				resolve(results);
			});
		});
	}
})();
