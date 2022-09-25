import Conf from 'conf';
import Table from 'cli-table3';
import chalk from 'chalk';
import fs from 'fs';
import utils from './utils';


const compare = async ( args ) => {

	let report = [];


	// Get the filepaths from cli params
	// 
	const filepath_left = ( args['l'] || args['left'] );
	const filepath_right = ( args['r'] || args['right'] );


	// Read files
	// 
	const content_left = utils.file_get_contents( filepath_left );
	const content_right = utils.file_get_contents( filepath_right );


	// Validate content lengths
	// 
	if ( !( content_left.length ) || ( content_left.length < 1 ) ) {
		console.error( 'Left dataset is empty or not found.' );
		return; }
	if ( !( content_right.length ) || ( content_right.length < 1 ) ) {
		console.error( 'Right dataset is empty or not found.' );
		return; }


	// Compare content lengths
	// 
	if ( content_left.length !== content_right.length ) {
		console.log( chalk.greenBright( 'Content lengths are not identical:' ) );
		let table = new Table({
			head: ['Left', 'Right'],
			colWidths: [50, 50],
			wordWrap: true
		});
		table.push([
			content_left.length,
			content_right.length
		]);
		console.log(table.toString()); }
	else {
		console.log( chalk.greenBright( `Content lengths are identical: ${content_left.length}` ) ); }


	// Read as an object of objects
	// 
	const left = utils.dataset_to_object( content_left, 'tab' );
	const right = utils.dataset_to_object( content_right, 'tab' );
	
	const cols_left = Object.keys( Object.values( left )[0] );
	const cols_right = Object.keys( Object.values( right )[0] );
	
	const col_count_left = cols_left.length;
	const col_count_right = cols_right.length;

	const row_count_left = Object.values( left ).length;
	const row_count_right = Object.values( right ).length;


	// Validate col & row counts
	// 
	if ( !( col_count_left) || ( col_count_left < 1 ) ) {
		console.error( 'Left dataset could not be processed.' );
		return; }
	if ( !( col_count_right) || ( col_count_right < 1 ) ) {
		console.error( 'Right dataset could not be processed.' );
		return; }
	if ( !( row_count_left) || ( row_count_left < 1 ) ) {
		console.error( 'Left dataset could not be processed.' );
		return; }
	if ( !( row_count_right) || ( row_count_right < 1 ) ) {
		console.error( 'Right dataset could not be processed.' );
		return; }


	// Compare row counts
	// 
	if ( row_count_left !== row_count_right ) {
		console.log( chalk.greenBright( 'Number of rows are not identical:' ) );
		let table = new Table({
			head: ['Left', 'Right'],
			colWidths: [50, 50],
			wordWrap: true
		});
		table.push([
			row_count_left,
			row_count_right
		]);
		console.log(table.toString()); }
	else {
		console.log( chalk.greenBright( `Row counts are equal: ${row_count_left}` ) ); }


	// Compare column counts
	// 
	if ( cols_left.length !== cols_right.length ) {
		console.log( chalk.greenBright( 'Number of columns are not identical:' ) );
		let table = new Table({
			head: ['Left', 'Right'],
			colWidths: [50, 50],
			wordWrap: true
		});
		table.push([
			col_count_left,
			col_count_right
		]);
		console.log(table.toString()); }
	else {
		console.log( chalk.greenBright( `Column counts are identical: ${col_count_left}` ) ); }


	// Compare column names
	// 
	const cols_shared = utils.array_intersect( cols_left, cols_right );
	const cols_only_in_left = utils.array_diff( cols_left, cols_shared );
	const cols_only_in_right = utils.array_diff( cols_right, cols_shared );

	if ( ( cols_only_in_left.length > 0 ) || ( cols_only_in_right.length > 0 ) ) {
		console.log( chalk.greenBright( 'Column names are not identical:' ) );
		let table = new Table({
			head: ['Shared', 'Only in Left', 'Only in Right'],
			colWidths: [50, 50, 50],
			wordWrap: true
		});
		table.push([
			cols_shared.join( ', ' ),
			cols_only_in_left.join( ', ' ),
			cols_only_in_right.join( ', ' )
		]);
		console.log(table.toString()); }
	else {
		console.log( chalk.greenBright( `Column names are identical: ${cols_left.join( ', ' )}` ) ); }


	// Compare cells
	// only values of shared ids and cols
	// 
	
	let diff_cells = new Table({
		head: ['ID', 'Column', 'Value in Left', 'Value in Right'],
		colWidths: [50, 50, 50],
		wordWrap: true
	});

	for ( const key_left in left ) {
		const obj_left = left[key_left];
		if ( !( obj_left ) ) {
			// left is source of truth, thus skip record if failure
			// 
			continue; }

		const obj_right = right[key_left];
		if ( !( obj_right ) ) {
			diff_cells.push([
				key_left,
				'skipped',
				'skipped',
				'KEY MISSING'
			]);
			continue;
		}

		for ( const col of cols_shared ) {
			const val_left = obj_left[col];
			if ( !( val_left ) ) {
				// left is source of truth, thus skip record if failure
				// 
				continue; }

			const val_right = obj_right[col];
			if ( !( val_right ) ) {
				diff_cells.push([
					key_left,
					col,
					val_left,
					'VALUE MISSING'
				]);
				continue;
			}

			if ( val_right != val_left ) {
				diff_cells.push([
					key_left,
					col,
					val_left,
					val_right
				]);
				continue;
			}

		}

	}

	console.log( chalk.greenBright( 'Keys missing and value diffs in right dataset:' ) );
	console.log( diff_cells.toString() );

};


export default {
	compare
}