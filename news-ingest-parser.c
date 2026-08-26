#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/inotify.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#define TOP_SOURCES_AMOUNT 10
#define HASH_MAP_INITIAL_CAPACITY 8192
#define QUEUE_CAPACITY 256
#define EVENT_SIZE ( sizeof( struct inotify_event ) )
#define BUF_LEN ( 1024 * ( EVENT_SIZE + 16 ) )

struct string_view{
	const char *data;
	size_t length;
};

struct hash_entry{
	struct string_view domain;
	size_t count;
};

struct hash_map{
	struct hash_entry *entries;
	size_t capacity;
	size_t size;
	pthread_mutex_t mutex;
};

typedef struct{
	off_t offset;
	size_t length;
} log_task_t;

typedef struct{
	log_task_t *tasks;
	size_t capacity;
	size_t head;
	size_t tail;
	size_t count;
	
	pthread_mutex_t mutex;
	pthread_cond_t cond_not_empty;
	pthread_cond_t cond_not_full;
	bool stop_workers;
} work_queue_t;

struct thread_worker_data{
	work_queue_t *queue;
	struct hash_map *global_map;
	int fd;
};

void hash_map_init( struct hash_map *map ){
	assert( map != NULL );
	map->capacity = HASH_MAP_INITIAL_CAPACITY;
	map->size = 0;
	map->entries = calloc( map->capacity, sizeof( struct hash_entry ) );
	pthread_mutex_init( &map->mutex, NULL );
}

void hash_map_destroy( struct hash_map *map ){
	assert( map != NULL );
	for( size_t i = 0; i < map->capacity; i++ ){
		if( map->entries[ i ].domain.data != NULL ){
			free( ( void * ) map->entries[ i ].domain.data );
		}
	}
	free( map->entries );
	pthread_mutex_destroy( &map->mutex );
	memset( map, 0, sizeof( *map ) );
}

bool strings_equal( struct string_view a, struct string_view b ){
	if( a.length != b.length ){
		return false;
	}
	return memcmp( a.data, b.data, a.length ) == 0;
}

uint64_t hash_string_view( struct string_view sv ){
	uint64_t hash = 14695981039346656037ULL;
	for( size_t i = 0; i < sv.length; i++ ){
		hash ^= ( unsigned char ) sv.data[ i ];
		hash *= 1099511628211ULL;
	}
	return hash;
}

bool _hash_map_grow( struct hash_map *map ){
	size_t old_capacity = map->capacity;
	struct hash_entry *old_entries = map->entries;

	map->capacity *= 2;
	map->entries = calloc( map->capacity, sizeof( struct hash_entry ) );
	
	if( map->entries == NULL ){
		map->entries = old_entries;
		map->capacity = old_capacity;
		return false;
	}

	map->size = 0;

	for( size_t i = 0; i < old_capacity; i++ ){
		if( old_entries[ i ].domain.data != NULL ){
			uint64_t hash = hash_string_view( old_entries[ i ].domain );
			size_t index = hash & ( map->capacity - 1 ); 
			
			while( map->entries[ index ].domain.data != NULL ){
				index = ( index + 1 ) & ( map->capacity - 1 );
			}
			map->entries[ index ] = old_entries[ i ];
			map->size++;
		}
	}

	free( old_entries );
	return true;
}

bool hash_map_insert_or_update( struct hash_map *map, struct string_view domain ){
	pthread_mutex_lock( &map->mutex );
	
	if( map->size * 2 >= map->capacity ){
		if( !_hash_map_grow( map ) ){
			pthread_mutex_unlock( &map->mutex );
			return false;
		}
	}

	uint64_t hash = hash_string_view( domain );
	size_t index = hash & ( map->capacity - 1 );

	while( map->entries[ index ].domain.data != NULL ){
		if( strings_equal( map->entries[ index ].domain, domain ) ){
			map->entries[ index ].count++;
			pthread_mutex_unlock( &map->mutex );
			return true;
		}
		index = ( index + 1 ) & ( map->capacity - 1 );
	}

	char *persistent_str = malloc( domain.length );
	memcpy( persistent_str, domain.data, domain.length );

	map->entries[ index ].domain.data = persistent_str;
	map->entries[ index ].domain.length = domain.length;
	map->entries[ index ].count = 1;
	map->size++;
	
	pthread_mutex_unlock( &map->mutex );
	return true;
}

void queue_init( work_queue_t *q, size_t capacity ){
	q->tasks = malloc( capacity * sizeof( log_task_t ) );
	q->capacity = capacity;
	q->head = 0;
	q->tail = 0;
	q->count = 0;
	q->stop_workers = false;
	pthread_mutex_init( &q->mutex, NULL );
	pthread_cond_init( &q->cond_not_empty, NULL );
	pthread_cond_init( &q->cond_not_full, NULL );
}

void queue_push( work_queue_t *q, log_task_t task ){
	pthread_mutex_lock( &q->mutex );
	
	while( q->count == q->capacity && !q->stop_workers ){
		pthread_cond_wait( &q->cond_not_full, &q->mutex );
	}
	
	q->tasks[ q->tail ] = task;
	q->tail = ( q->tail + 1 ) % q->capacity;
	q->count++;
	
	pthread_cond_signal( &q->cond_not_empty );
	pthread_mutex_unlock( &q->mutex );
}

void queue_destroy( work_queue_t *q ){
	free( q->tasks );
	pthread_mutex_destroy( &q->mutex );
	pthread_cond_destroy( &q->cond_not_empty );
	pthread_cond_destroy( &q->cond_not_full );
}

void *worker_loop( void *arg ){
	struct thread_worker_data *data = ( struct thread_worker_data * ) arg;
	work_queue_t *q = data->queue;
	int fd = data->fd;
	
	while( 1 ){
		pthread_mutex_lock( &q->mutex );
		
		while( q->count == 0 && !q->stop_workers ){
			pthread_cond_wait( &q->cond_not_empty, &q->mutex );
		}
		
		if( q->stop_workers && q->count == 0 ){
			pthread_mutex_unlock( &q->mutex );
			break; 
		}
		
		log_task_t task = q->tasks[ q->head ];
		q->head = ( q->head + 1 ) % q->capacity;
		q->count--;
		
		pthread_cond_signal( &q->cond_not_full );
		pthread_mutex_unlock( &q->mutex );
		
		char *buffer = malloc( task.length );
		if( buffer == NULL ) continue;

		ssize_t bytes_read = pread( fd, buffer, task.length, task.offset );
		
		if( bytes_read > 0 ){
			const char *cursor = buffer;
			const char *buffer_end = buffer + bytes_read;

			while( cursor < buffer_end ){
				const char *pipe1 = memchr( cursor, '|', buffer_end - cursor );
				if( pipe1 == NULL ) break;
				cursor = pipe1 + 1;
				
				const char *pipe2 = memchr( cursor, '|', buffer_end - cursor );
				if( pipe2 == NULL ) break;
				cursor = pipe2 + 1;

				const char *pipe3 = memchr( cursor, '|', buffer_end - cursor );
				if( pipe3 == NULL ) break;
				size_t domain_length = pipe3 - cursor;

				if( domain_length > 0 ){
					struct string_view current_domain = { cursor, domain_length };
					hash_map_insert_or_update( data->global_map, current_domain );
				}
				
				cursor = pipe3 + 1;

				const char *newline = memchr( cursor, '\n', buffer_end - cursor );
				if( newline == NULL ) break;
				cursor = newline + 1;
			}
		}
		free( buffer );
	}
	return NULL;
}

int compare_counts( const void *a, const void *b ){
	const struct hash_entry *stat_a = ( const struct hash_entry * ) a;
	const struct hash_entry *stat_b = ( const struct hash_entry * ) b;

	if( stat_a->count < stat_b->count ){
		return 1;
	}
	if( stat_a->count > stat_b->count ){
		return -1;
	}
	return 0;
}

void print_top_sources( struct hash_map *map, size_t top_n ){
	pthread_mutex_lock( &map->mutex );
	
	struct hash_entry *dense_stats = malloc( map->size * sizeof( struct hash_entry ) );
	size_t idx = 0;
	
	for( size_t i = 0; i < map->capacity; i++ ){
		if( map->entries[ i ].domain.data != NULL ){
			dense_stats[ idx ] = map->entries[ i ];
			idx++;
		}
	}

	pthread_mutex_unlock( &map->mutex );

	qsort( dense_stats, map->size, sizeof( struct hash_entry ), compare_counts );

	size_t limit = map->size < top_n ? map->size : top_n;
	
	printf( "\033[2J\033[H" ); 
	printf( "--- Live Top %zu News Sources ---\n", limit );
	
	for( size_t i = 0; i < limit; i++ ){
		printf( "%8zu  %.*s\n", 
				dense_stats[ i ].count, 
				( int ) dense_stats[ i ].domain.length, 
				dense_stats[ i ].domain.data );
	}
	
	free( dense_stats );
}

int main( int argc, char *argv[] ){
	if( argc != 2 ){
		fprintf( stderr, "usage: %s <logfile.psv>\n", argv[ 0 ] );
		return EXIT_FAILURE;
	}

	int fd = open( argv[ 1 ], O_RDONLY );
	if( fd == -1 ){
		perror( "Failed to open file" );
		return EXIT_FAILURE;
	}

	struct hash_map global_map;
	hash_map_init( &global_map );

	work_queue_t work_queue;
	queue_init( &work_queue, QUEUE_CAPACITY );

	long num_cores = sysconf( _SC_NPROCESSORS_ONLN );
	if( num_cores < 1 ) num_cores = 1;

	pthread_t *threads = malloc( num_cores * sizeof( pthread_t ) );
	struct thread_worker_data worker_data = { &work_queue, &global_map, fd };

	for( long i = 0; i < num_cores; i++ ){
		pthread_create( &threads[ i ], NULL, worker_loop, &worker_data );
	}

	int inotify_fd = inotify_init();
	if( inotify_fd < 0 ){
		perror( "inotify_init" );
		return EXIT_FAILURE;
	}

	int wd = inotify_add_watch( inotify_fd, argv[ 1 ], IN_MODIFY );
	if( wd == -1 ){
		perror( "inotify_add_watch" );
		return EXIT_FAILURE;
	}

	struct stat file_stat;
	fstat( fd, &file_stat );
	off_t current_offset = 0;

	if( file_stat.st_size > 0 ){
		log_task_t initial_task = { 0, file_stat.st_size };
		queue_push( &work_queue, initial_task );
		current_offset = file_stat.st_size;
	}

	char event_buffer[ BUF_LEN ];
	printf( "Monitoring %s for changes with %ld worker threads...\n", argv[ 1 ], num_cores );

	while( 1 ){
		int length = read( inotify_fd, event_buffer, BUF_LEN );
		if( length < 0 ){
			perror( "read" );
			break;
		}

		int i = 0;
		while( i < length ){
			struct inotify_event *event = ( struct inotify_event * ) &event_buffer[ i ];
			if( event->mask & IN_MODIFY ){
				fstat( fd, &file_stat );
				if( file_stat.st_size > current_offset ){
					size_t new_bytes = file_stat.st_size - current_offset;
					log_task_t task = { current_offset, new_bytes };
					queue_push( &work_queue, task );
					current_offset = file_stat.st_size;
					
					print_top_sources( &global_map, TOP_SOURCES_AMOUNT );
				}
			}
			i += EVENT_SIZE + event->len;
		}
	}

	close( inotify_fd );
	close( fd );
	queue_destroy( &work_queue );
	hash_map_destroy( &global_map );
	free( threads );

	return EXIT_SUCCESS;
}
