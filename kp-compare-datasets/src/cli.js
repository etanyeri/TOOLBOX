import minimist from 'minimist';
import { version } from './version';
import main from './main';

export async function cli( args_array ) {
	
	const args = minimist( args_array.slice( 2 ) );
	const cmd = args._[0];

	switch ( cmd ) {
		
		case 'version':
		case 'v':
			version( args );
			break;

		default:
			main.compare( args );
			break;

	}

}
