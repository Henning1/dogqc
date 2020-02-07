#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>


void* malloc_memory_mapped_file ( size_t size, const char *filepath) {
    /* Open a file for writing.
     *  - Creating the file if it doesn't exist.
     *  - Truncating it to 0 size if it already exists. (not really needed)
     *
     * Note: "O_WRONLY" mode is not sufficient when mmaping.
     */
    int fd = open(filepath, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600);
    if (fd == -1)
    {
        ERROR("opening file")
    }

    // Write filesize to first index for later unmap
    size_t filesize = sizeof(size) + size + 1; // len + content + \0 character
    if (write(fd, &filesize, 4) == -1)
    {
        close(fd);
        ERROR("writing file len (size_t) to first byte of the file");
    }

    // Stretch the file size to the size of the (mmapped) array of char
    if (lseek(fd, filesize-1, SEEK_SET) == -1)
    {
        close(fd);
        ERROR("calling lseek() to 'stretch' the file");
    }

    /* Something needs to be written at the end of the file to
     * have the file actually have the new size.
     * Just writing an empty string at the current file position will do.
     *
     * Note:
     *  - The current position in the file is at the end of the stretched 
     *    file due to the call to lseek().
     *  - An empty string is actually a single '\0' character, so a zero-byte
     *    will be written at the last byte of the file.
     */
    if (write(fd, "", 1) == -1)
    {
        close(fd);
        ERROR("writing last byte of the file");
    }
  
    // Map file into memory
    char* map = (char*)mmap(0, filesize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED)
    {   
        close(fd);
        ERROR("mmapping file");
    }

    // Close file ( remains mapped )
    close(fd);
    return map+8;
}


void* map_memory_file ( const char* filepath ) {
    int fd = open(filepath, O_RDWR, (mode_t)0600);
    if (fd == -1) {   
        ERROR("Opening file")
    }
    size_t filesize;
    read(fd, &filesize, 8);
    char* map = (char*)mmap(0, filesize, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {   
        close(fd);
        ERROR("mmapping the file");
    }
    return (void*)(map+8);
}


void unmap_memory_file ( void* ptr ) {
    size_t* baseptr = ((size_t*)(ptr))-1;
    size_t size = *(baseptr);
    printf("Unmapping file with size %zu", size);    
    if (munmap(baseptr, size) == -1)
    {
        ERROR("un-mmapping file");
    }
}


void free_memory_mapped_file ( const char* filepath ) {
  int ret = remove(filepath);
   if(ret == 0) {
      printf("File deleted successfully");
   } else {
      ERROR("deleting file");
   }
}
