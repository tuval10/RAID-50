/*	
*	implementing a raid-50 disk-complex
* 	usage example:
*	sudo ./raid0 /dev/sdb /dev/sdc
*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h> // for open flags
#include <assert.h>
#include <errno.h> 
#include <string.h>
#include <limits.h>
#include <unistd.h>

#define SECTOR_SIZE 512
#define SECTORS_PER_BLOCK 2
#define BLOCK_SIZE (SECTOR_SIZE * SECTORS_PER_BLOCK)
#define DEVICE_SIZE (BLOCK_SIZE * 1024 * 256) // assume all devices identical in size
#define BLOCKS_PER_DEVICE (DEVICE_SIZE/BLOCK_SIZE)
#define BUFFER_SIZE (BLOCK_SIZE * 256)

char	buf[BUFFER_SIZE] = {0};
char	**devices_name;
int		r0, r5, num_dev, *dev_fd;

void do_raid50_rw(char* operation, int sector, int count);
void do_raid50_setbuf(char c);
void do_raid50_repair(int dev_num);
int get_r5_killed_device_num(int r5_devices_array);
void kill(int dev_num);

int exit_with_message(){
	int i;
	//closing the devices.
	for(i=0; i < r5 * r0; i++) {
		if (dev_fd[i] >= 0)
			assert(!close(dev_fd[i]));
    }
	//
	free(dev_fd);
	return 0;
}

void read_from_dead_disk(int r5_dev ,int offset, int size, int dead_disk_index, int sector_start, int sector_end){
	char tempBuffer[BLOCK_SIZE] = {0}, tempRead[BLOCK_SIZE];
	int i,j, currDevice;
	for(i = 0; i < r5; i++){
		currDevice = r5_dev * r5 + i;
		if( currDevice == dead_disk_index) continue;
		if((offset != lseek(dev_fd[currDevice], offset, SEEK_SET)) || (size != read( dev_fd[currDevice], tempRead, size)))
			assert(exit_with_message() && "More devices failed than can be recovered");
		else
			printf("Operation on device %d, sector %d-%d\n", currDevice, sector_start, sector_end);
		for(j = 0 ; j < BLOCK_SIZE ; j++){  //xoring together
			tempBuffer[j] = tempBuffer[j] ^ tempRead[j];
		}
	}
	for(j = 0 ; j < BLOCK_SIZE ; j++){  //copying to the buffer
		buf[j] = tempBuffer[j];
	}
}

void update_parity(int parity_dev_num, int offset, int size, int dead_devices_in_r5, int sector_start, int sector_end){
	int j;
	char parity[BLOCK_SIZE];
	if((offset != lseek(dev_fd[parity_dev_num], offset, SEEK_SET)) || (size != read( dev_fd[parity_dev_num], parity, size))){
		kill(parity_dev_num);
		return;
	}
	else
		printf("Operation on device %d, sector %d-%d\n", parity_dev_num, sector_start, sector_end);
	// xoring
	for(j = 0; j < size; j++)
		parity[j] = parity[j] ^ buf[j];
	// writing updated parity values
	if((offset != lseek(dev_fd[parity_dev_num], offset, SEEK_SET)) || (size != write( dev_fd[parity_dev_num], parity, size))){
		kill(parity_dev_num);
		return;
	}
	else
		printf("Operation on device %d, sector %d-%d\n", parity_dev_num, sector_start, sector_end);
}

void do_raid50_rw(char* operation, int sector, int count){
	int i, block_num, strip_num, r5_dev, dev_num, parity_dev_num, parity_block_in_strip, writing_block_in_strip;
	int block_start, sector_start, num_sectors, offset, size, sectors_left;
	for (i = sector; i < sector+count; i += num_sectors) {
		// find the relevant device for current sector
		block_num = i / SECTORS_PER_BLOCK;
		strip_num = (block_num / (r5-1));
		r5_dev = strip_num % r0;
		parity_block_in_strip = (r5-1) -  ((strip_num / r0) % r5);  //where is the parity block in the strip (0 to r5 -1)
		writing_block_in_strip = block_num - ((strip_num) *  (r5 - 1)) ;
		if(parity_block_in_strip <= writing_block_in_strip)
			writing_block_in_strip++;
		dev_num = r5_dev * r5 + writing_block_in_strip;
		parity_dev_num = r5_dev * r5 + parity_block_in_strip;

		// find offset of sector inside device (downwards)
		block_start = strip_num / r0; //index inside the device (downwards)
		sector_start = i % SECTORS_PER_BLOCK;
		offset = ((block_start * SECTORS_PER_BLOCK) + sector_start) * SECTOR_SIZE;
		sectors_left = sector+count-i;
		num_sectors = (sectors_left >  SECTORS_PER_BLOCK - sector_start) ? (SECTORS_PER_BLOCK - sector_start) : sectors_left;
		size = num_sectors * SECTOR_SIZE;
		
		// validate calculations
		assert(num_sectors > 0 && "no sectors to write");
		assert(size <= BLOCK_SIZE && "writing size is bigger than block size");
		assert(size <= BUFFER_SIZE && "writing size is bigger than buffer size");
		assert(offset+size <= DEVICE_SIZE && "writing exceed device size");
			
		if (!strcmp(operation, "READ")){
			if(dev_fd[dev_num] >=0){ //regular reading
				if((offset != lseek(dev_fd[dev_num], offset, SEEK_SET)) || (size != read( dev_fd[dev_num], buf, size))){
					kill(dev_num);
				}
				else
					printf("Operation on device %d, sector %d-%d\n", dev_num, sector_start, sector_start + num_sectors -1);
			}
			if(dev_fd[dev_num] < 0){ //reading from a dead disk
				read_from_dead_disk(r5_dev ,offset, size, dev_num, sector_start, sector_start + num_sectors - 1);
			}
			printf("got: %d\n", *buf);
		}
		else if (!strcmp(operation, "WRITE")){
			if(dev_fd[dev_num] >=0){ //regular writing
				if((offset != lseek(dev_fd[dev_num], offset, SEEK_SET)) || (size != write( dev_fd[dev_num], buf, size))){
					kill(dev_num);
				}
				else
					printf("Operation on device %d, sector %d-%d\n", dev_num, sector_start, sector_start + num_sectors -1);
			}
			update_parity(parity_dev_num, offset, size, ((dev_fd[dev_num] >=0) ? 0 : 1) , sector_start, sector_start + num_sectors - 1);
		}
	}
}

void do_raid50_repair(int dev_num){
	int i,j, location_in_r5_device, first_dev_num, offset = 0, curr_disc;
	char buf2[BUFFER_SIZE], chartemp = buf[0];
	printf("reopening device num %d: %s\n", dev_num, devices_name[dev_num]);
	if(dev_fd[dev_num] < 0)
		assert(((dev_fd[dev_num] = open(devices_name[dev_num], O_RDWR)) >= 0) && "Couldn't repair. exits");
	else return;
	do_raid50_setbuf(0);
	location_in_r5_device = dev_num %r5;
	first_dev_num = dev_num - location_in_r5_device;
	assert(DEVICE_SIZE % BUFFER_SIZE == 0 && "buffer size and device size mismatch");
	for( offset = 0; offset < DEVICE_SIZE ; offset += BUFFER_SIZE){
		for(i = 0; i < r5; i++){
			curr_disc = first_dev_num + i;
			if(curr_disc == dev_num) continue;
			assert(offset == lseek(dev_fd[curr_disc], offset, SEEK_SET) && "Couldn't repair. exits");
			assert(BUFFER_SIZE == read(dev_fd[curr_disc],buf2, BUFFER_SIZE ) && "Couldn't repair. exits");
			for(j = 0; j < BUFFER_SIZE; j++) 
				buf[j] ^= buf2[j]; 
		}
		assert((offset == lseek(dev_fd[dev_num], offset, SEEK_SET)) && "Couldn't repair. exits");
		assert(BUFFER_SIZE == write(dev_fd[dev_num],buf, BUFFER_SIZE) && "Couldn't repair. exits");
		//printf("%d offset. left %d\n", offset, (DEVICE_SIZE - offset));
	}
	do_raid50_setbuf(chartemp);
	printf("repaired device %d successfully\n", dev_num);
}

void do_raid50_setbuf(char c){
	int i;
	assert( CHAR_BIT == 8 );
	for(i=0; i< BUFFER_SIZE; i++)
		buf[i] = c;
}

// gets a number between 0 to r0-1. return number of killed devices in this r5-devices array
int get_r5_killed_device_num(int r5_devices_array){
	int start_device = r5_devices_array * r5 ;
	int killed_devices_num = 0;
	int i;
	for(i = 0 ; i < r5; i++){
		if(dev_fd[start_device+i] == -1)
			killed_devices_num++;
	}
	return killed_devices_num;
}

void kill(int dev_num){
	int close_value;
	assert(dev_fd[dev_num] != -1 && "cannot kill a dead device");
	assert( close( dev_fd[dev_num]) == 0 && "problem closing malfunctioned device");
	dev_fd[dev_num] = -1 ;
	assert( get_r5_killed_device_num(dev_num % r5) < 2 && "More devices failed than can be recovered");
}

int main(int argc, char** argv){
	int i,j, not_opened_devices_num;
	char line[1024];
	// vars for parsing input line
	char operation[20];
	int sector;
	int count;

	assert(argc > 4);
	r5 = atoi(argv[1]);
	r0 = atoi(argv[2]);
	devices_name = &argv[3];
	num_dev = argc-3;
	assert(r0 * r5 == num_dev);
	assert((dev_fd = (int *) calloc(num_dev, sizeof(int))) != NULL);
	
	// open all devices
	for (i = 0; i < num_dev; ++i) {
		printf("Opening device %d: %s\n", i, argv[i+3]);
		dev_fd[i] = open(argv[i+3], O_RDWR);
	}
	for (i = 0; i < r0 -1; ++i) {
		assert( get_r5_killed_device_num(i) < 2 && "More devices failed than can be recovered");
	}
	// read input lines to get command of type "OP <SECTOR> <COUNT>"
	while (fgets(line, 1024, stdin) != NULL) {
		assert((sscanf(line, "%s %d %d", operation, &sector, &count) == 3) && "insert 3 arguments");
		if (!strcmp(operation, "KILL")) {
			kill(sector);
		}
		// REPAIR
		else if (!strcmp(operation, "REPAIR")) {
			do_raid50_repair(sector);
		}
		// SETBUF
		else if (!strcmp(operation, "SETBUF")) {
			do_raid50_setbuf( (char)(sector) ) ;
		}
		// READ / WRITE
		else {
			do_raid50_rw(operation, sector, count);
		}
	}
	//closing the devices.
	for(i=0; i < num_dev -1; i++) {
		if (dev_fd[i] >= 0)
			assert(!close(dev_fd[i]));
    }
	free(dev_fd);
	return 0;
}
