import axios from 'axios';
import chalk from 'chalk';
import fs from 'fs';


const file_exists = async ( path ) => !!( await fs.promises.stat( path ).catch( e => false ) );

const file_readable = async( path ) => !!( await fs.promises.access( path, fs.constants.W_OK ).catch( e => false ) );


/**
 * 
 * Reads the file synchronously into a buffer then a string
 * 
 * @param  {[string]} path Filepath (relative or absolute) to the file to be read
 * @return {[string]}      Contents of file or empty string if failure
 */
const file_get_contents = ( path ) => {
	
	let content = '';
	
	try {
		content = fs.readFileSync( path ).toString();
	} catch(err) {
		content = '';
	}

	return content;

}

/**
 * 
 * Converts a tab-delimited file into an array of objects
 * with keys read from the header row
 * 
 * @param  {[string]} input A tab-delimited file
 * @return {[array]}       Array of objects
 * 
 */
const dataset_to_array = ( input, delimiter ) => {

	if ( 'string' !== typeof input ) {
		return 'no input'; }

	if ( 'string' !== typeof delimiter ) {
		delimiter = "\t"; }
	if ( 'tab' == delimiter ) {
		delimiter = "\t"; }

	// Split into rows
	// 
	let rows = input.split( "\n" );
	const keys = rows.shift().split( delimiter );

	let items = [];
	for ( const row of rows ) {
		const cols = row.split( "\t" );
		if ( keys.length !== cols.length ) {
			continue; }

		let item = {};
		for ( let i in keys ) {
			const key = keys[i];
			const col = cols[i];
			item[key] = col;
		}

		items.push( item );

	}

	return items;

}


/**
 * 
 * Converts a tab-delimited file into an array-like object of objects
 * with keys read from the header row
 * and first one with the unique key
 * 
 * @param  {[string]} input A tab-delimited file
 * @return {[object]}       Object of objects
 * 
 */
const dataset_to_object = ( input, delimiter ) => {

	if ( 'string' !== typeof input ) {
		return 'no input'; }

	if ( 'string' !== typeof delimiter ) {
		delimiter = "\t"; }
	if ( 'tab' == delimiter ) {
		delimiter = "\t"; }

	// Split into rows
	// 
	let rows = input.split( "\n" );
	const keys = rows.shift().split( delimiter );
	const unique_key = keys.shift();

	let items = {};
	for ( const row of rows ) {
		
		let cols = row.split( "\t" );
		const unique_val = cols.shift();

		if ( keys.length !== cols.length ) {
			continue; }

		let item = {};
		for ( let i in keys ) {
			const key = keys[i];
			const col = cols[i];
			item[key] = col;
		}

		items[`${unique_key}__${unique_val}`] = item;

	}

	return items;

}


/**
 * 
 * Values existing in both arrays
 * 
 * @param  {[array]} a [description]
 * @param  {[array]} b [description]
 * @return {[array]}   [description]
 */
const array_intersect = ( a, b ) => {

	const set_a = new Set( a );
	const set_b = new Set( b );
	
	const set_c = new Set( [...set_a].filter( x => set_b.has( x ) ) );

	const intersection = Array.from( set_c );

	return intersection;

}


/**
 * 
 * Values existing in a but not in b
 * 
 * @param  {[array]} a [description]
 * @param  {[array]} b [description]
 * @return {[array]}   [description]
 */
const array_diff = ( a, b ) => {

	const set_a = new Set( a );
	const set_b = new Set( b );
	
	const set_c = new Set( [...set_a].filter( x => !( set_b.has( x ) ) ) );

	const diff = Array.from( set_c );

	return diff;

}


export default {
	file_exists,
	file_get_contents,
	array_intersect,
	array_diff,
	dataset_to_array,
	dataset_to_object
}