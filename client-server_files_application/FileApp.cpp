
#include "prints.h"
#include <stdio.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <cstdlib>
#include <cstring>
#include <arpa/inet.h>
#include <iostream>
#include <sys/stat.h>
#include <fstream>
#include <unistd.h>
#include <sstream>
#include <zconf.h>


#define DIR_NAME_MAX_LEN 4095
#define FILE_NAME_MAX_LEN 255

#define MAX_SOCKETS 3

#define SERVER "-s"
#define CLIENT_UPLOAD "-u"
#define CLIENT_DOWNLOAD "-d"

#define COMMAND_INDEX 1
#define CLIENT_LOCAL_INDEX 2
#define CLIENT_REMOTE_INDEX 3
#define SERVER_LOCAL_INDEX 2
#define SERVER_PORT_INDEX 3
#define CLIENT_PORT_INDEX 4

#define SERVER_ID_INDEX 5


const char * SUCCESS = "success";
const char * FAILURE = "failure";

fd_set results_set;



/**
 * the function creates, initializes the server and listens to clients' requests.
 * @param portNum - server's port number.
 * @return 0 on success, -1 on failure.
 */
int establish(u_short portNum) {

    char host_name[HOST_NAME_MAX + 1];
    int listen_socket;
    struct sockaddr_in server_sock_addr;
    struct hostent *hp;
    char IP[INET_ADDRSTRLEN] = {0};

    /* hostent initialization */
    gethostname(host_name, HOST_NAME_MAX);            /* who are we? */
    hp = gethostbyname(host_name);                  /* get our address info */
    if (hp == NULL) {                             /* we don't exist !? */
        return (-1);
    }

    /* socket initialization */                     /* clear our address */
    memset(&server_sock_addr, 0, sizeof(struct sockaddr_in));   /* clear our address */
    server_sock_addr.sin_family = hp->h_addrtype;              /* this is our host address */
    memcpy(&server_sock_addr.sin_addr, hp->h_addr, hp->h_length);
    server_sock_addr.sin_port = htons(portNum);                /* this is our port number */

    if ((listen_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {     /* create sock */
        return (-1);
    }

    if (bind(listen_socket, (struct sockaddr *)&server_sock_addr,sizeof(struct sockaddr_in)) < 0) {
        close(listen_socket);
        return(-1);                               /* bind address to sock */
    }
    else {
        inet_ntop(AF_INET, &server_sock_addr.sin_addr, IP, sizeof(IP));
        printf(SERVERS_BIND_IP_STR, IP);
    }

    listen(listen_socket, MAX_SOCKETS);             /* max num of queued connects */
    printf(WAIT_FOR_CLIENT_STR);

    FD_ZERO(&results_set);
    FD_SET(listen_socket, &results_set);
    FD_SET(STDIN_FILENO, &results_set);

    return(listen_socket);
}


/**
 * the function waits for a connection to occur on a socket created with establish().
 * @param sock - socket created with establish() = server listening socket
 * @return the file descriptor of the new socket of the connection.
 */
int get_connection(int listen_sock){
    struct sockaddr_in client_addr;     /* address of socket */
    memset(&client_addr, 0, sizeof(client_addr));
    socklen_t client_len = sizeof(client_addr);                  /* size of address */
    int new_connection;            /* socket of connection */

    getsockname(listen_sock, (struct sockaddr *)&client_addr, &client_len);            /* for accept() */
    new_connection = accept(listen_sock, (struct sockaddr *)&client_addr, &client_len);

    if (new_connection < 0) {   /* accept connection if there is one */
        return (-1);
    }

    char client_ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);      // get client IP
    printf(CLIENT_IP_STR, client_ip);

    FD_SET(new_connection, &results_set);

    return(new_connection);
}

/**
 * the function creates a new socket for the connection created.
 * @param host_name - server's host name
 * @param port_num - server's port number
 * @return the file descriptor of the new socket of the connection.
 */
int call_socket(char *host_name, int port_num) {
    struct sockaddr_in sock_addr;
    struct hostent *hp;
    int connection_sock;

    if ((hp = gethostbyname(host_name)) == NULL) { /* do we know the host's address? */
        return(-1);                                /* no */
    }

    memset(&sock_addr, 0 , sizeof(sock_addr));
    memcpy((char *)&sock_addr.sin_addr, hp->h_addr, hp->h_length);   /* set address */
    sock_addr.sin_family = hp->h_addrtype;
    sock_addr.sin_port = htons((u_short)port_num);

    if ((connection_sock = socket(hp->h_addrtype, SOCK_STREAM, 0)) < 0) {   /* get socket */
        return (-1);
    }
    if (connect(connection_sock, (struct sockaddr *)&sock_addr,sizeof(sock_addr)) < 0) {                  /* connect */
        close(connection_sock);
        return(-1);
    }
    return(connection_sock);

}

/**
 * the function reads data from a socket into a buffer
 * @param sock - connected socket
 * @param buf - pointer to the buffer
 * @param bytes_num - number of characters (bytes) we want
 * @return total bytes number read
 */
int read_data(int sock, char buf[1024], int bytes_num) {
    int bytes_count = 0;            /* counts bytes read */
    int bytes_read = 0;              /* bytes read this pass */

    while (bytes_count < bytes_num) {             /* loop until full buffer */
        bytes_read = read(sock, buf, bytes_num - bytes_count);
        if (bytes_read > 0) {
            bytes_count += bytes_read;                /* increment byte counter */
            buf += bytes_read;                   /* move buffer ptr for next read */
        }
        else if (bytes_read < 1) {                    /* signal an error to the caller */
            return (bytes_read);
        }
        else {      /* br = 0 */
            //TODO close socket from server side?
        }
    }
    return(bytes_count);
}


/**
 * the function writes data from buffer to socket
 * @param sock - connected socket
 * @param buf - pointer to the buffer
 * @param bytes_num - number of characters (bytes) we want
 * @return total bytes number written
 */
int write_data(int sock, char *buf, int bytes_num){
    int bytes_count = 0;            /* counts bytes read */
    int bytes_write = 0;              /* bytes read this pass */

    while (bytes_count < bytes_num) {             /* loop until full buffer */
        bytes_write = write(sock, buf, bytes_num - bytes_count);
        if (bytes_write > 0) {
            bytes_count += bytes_write;                /* increment byte counter */
            buf += bytes_write;                   /* move buffer ptr for next read */
        }
        else if (bytes_write < 0) {
            perror("write_data");        /* signal an error to the caller */
            return(-1);
        }
        else {      /* br = 0 */
            //TODO close socket from server side?
        }
    }
    return(bytes_count);
}


long get_file_size(std::string file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}


bool send_file(int socket, char * src_file_name){

    char buf[1024];
    bzero(buf, sizeof(buf));
    FILE* fp;

    /* checks if file exists and can be read from */
    if( access(src_file_name, R_OK) == -1 ){
        printf(MY_FILE_ERROR_STR);
        write(socket, FAILURE, sizeof(FAILURE));
        return false;
    }

    /* opens the file */
    if((fp = fopen(src_file_name, "r")) == nullptr){
        printf(MY_FILE_ERROR_STR);
        write(socket, FAILURE, sizeof(FAILURE));
        return false;
    }
    write(socket, SUCCESS, sizeof(SUCCESS));

    /* checks the size of the file */
    long file_size = get_file_size(src_file_name);

    /* send the file's size */
    strcpy(buf, std::to_string(file_size).c_str());
    write(socket, buf, sizeof(buf));
    bzero(buf, sizeof(buf));

    /* read and send the file's content */
    rewind(fp);
    long writen_bites = 0;
    int to_write;

    while (file_size > writen_bites){
        to_write = std::min(1024l, (file_size - writen_bites));
        bzero(buf, sizeof(buf));
        fread(buf, to_write, sizeof(char), fp);
        if (write_data(socket, buf, to_write) <= 0){
            fclose(fp);
            return false;
        }
        writen_bites += to_write;
    }
    bzero(buf, sizeof(buf));
    if(read(socket, buf, sizeof(SUCCESS)) <= 0 || strcmp(buf, SUCCESS) != 0){
        fclose(fp);
        return false;
    }

    fclose(fp);
    return true;
}


bool get_file(int socket, char* file_name){
    char buf[1024];
    bzero(buf, sizeof(buf));

    if(read(socket, buf, sizeof(buf)) <= 0){
        return false;
    }
    if (strcmp(buf, SUCCESS) != 0){
        printf(REMOTE_FILE_ERROR_STR);
        return false;
    }
    bzero(buf, sizeof(buf));
    if(read(socket, buf, sizeof(buf)) <= 0){
        return false;
    }
    char * file_size_str = buf;
    long file_size = atol(file_size_str);

    FILE *file = fopen(file_name, "w");
    if(file == nullptr){
        return false;
    }
    bzero(buf, sizeof(buf));

    int bytes_num;
    long read_bytes = 0;
    int to_read;
    while (read_bytes < file_size){
        to_read = std::min(1024l, (file_size - read_bytes));
        bytes_num = read_data(socket, buf, to_read);
        if(bytes_num <= 0){
            fclose(file);
            bzero(buf, sizeof(buf));
            write(socket, FAILURE, sizeof(FAILURE));
            return false;
        }
        fwrite(buf, bytes_num, sizeof(char), file);
        read_bytes += bytes_num;
    }
    bzero(buf, sizeof(buf));
    write(socket, SUCCESS, sizeof(SUCCESS));

    fclose(file);
    return true;
}


void server_job(int listen_socket, char* local_dir) {
    fd_set reads_fd;
    char buf[1024];
    bzero(buf, sizeof(buf));
    std::string command;
    std::string remote;

    while (1) {
        reads_fd = results_set;
        int ready = select(MAX_SOCKETS + 1, &reads_fd, NULL, NULL, NULL);
        if (ready < 0) {
            perror("terminating server\n");
            exit(1);
        }

        if (FD_ISSET(listen_socket, &reads_fd)){

            int client = get_connection(listen_socket);
            if (client < 0) {
                perror("error - get connection");
                continue;
            }
            bzero(buf, sizeof(buf));

            // read the message from client and copy it in buffer
            if(read(client, buf, sizeof(buf)) <= 0){
                close(client);
                printf(FAILURE_STR);
                continue;
            }

            command = buf;
            printf(CLIENT_COMMAND_STR, buf[1]);

            bzero(buf, sizeof(buf));
            if(read(client, buf, sizeof(buf)) <= 0){
                close(client);
                printf(FAILURE_STR);
                continue;
            }
            remote = buf;

            printf(FILENAME_STR, buf);

            // build file path from server's directory and file name given by the client.
            char* file_path;
            file_path = (char *)malloc(sizeof(local_dir) + sizeof(buf) + 2); /* make space for the new string (should check the return value ...) */
            strcpy(file_path, local_dir); /* copy name into the new var */
            strcat(file_path, "/");
            strcat(file_path, remote.c_str());
            printf(FILE_PATH_STR, file_path);

            // checks validation of file name and file path.
            if(remote.size() > FILE_NAME_MAX_LEN || sizeof(file_path) > DIR_NAME_MAX_LEN || remote.find('/') != std::string::npos){
                write(client, FAILURE, sizeof(FAILURE)) ;
                printf(FILE_NAME_ERROR_STR);
                printf(FAILURE_STR);

            } else {
                write(client, SUCCESS, sizeof(SUCCESS));

                if (command == CLIENT_UPLOAD) {
                    if (!get_file(client, file_path)) {
                        printf(FAILURE_STR);
                    } else{
                        printf(SUCCESS_STR);
                    }
                } else if (command == CLIENT_DOWNLOAD) {
                    if (!send_file(client, file_path)) {
                        printf(FAILURE_STR);
                    } else{
                        printf(SUCCESS_STR);
                    }
                }

            }
            free(file_path);
            close(client);
            printf(WAIT_FOR_CLIENT_STR);
        }
        else if (FD_ISSET(STDIN_FILENO, &reads_fd)) {
            while (true) {
                bzero(buf, sizeof(buf));
                int n = 0;
                // copy server message in the buffer
                while ((buf[n++] = getchar()) != '\n');
                // if msg contains "Exit" then server exit and chat ended.
                if (strncmp("quit", buf, 4) == 0) {
                    close(listen_socket);
                    exit(0);
                }
            }
        }
    }
}

void send_args(int socket, char *command, char *remote){
    char buf[1024];
    bzero(buf, sizeof(buf));
    strcpy(buf, command);          // command
    write(socket, buf, sizeof(buf));
    bzero(buf, sizeof(buf));

    strcpy(buf, remote);          // remote
    write(socket, buf, sizeof(buf));
    bzero(buf, sizeof(buf));
}


int main(int argc , char *argv[]) {
    char buf[1024];
    int server, client;

    /* if the command line refers to server - create socket and listen */
    if (std::strcmp(argv[COMMAND_INDEX], SERVER) == 0) {
        server = establish(atoi(argv[SERVER_PORT_INDEX]));
        if (server < 0) {
            perror("ERROR in establish");
        }
        server_job(server, argv[SERVER_LOCAL_INDEX]);
        close(server);
    }


    else {
        client = call_socket(argv[SERVER_ID_INDEX], atoi(argv[CLIENT_PORT_INDEX]));
        if(client < 0){
            perror("didn't connect:");
            printf(FAILURE_STR);
            return -1;

        } else {
            printf(CONNECTED_SUCCESSFULLY_STR);

            send_args(client, argv[COMMAND_INDEX], argv[CLIENT_REMOTE_INDEX]);

            bzero(buf, sizeof(buf));
            if(read(client, buf, sizeof(SUCCESS)) <= 0){
                close(client);
                return -1;
            }

            if (strcmp(buf, SUCCESS) != 0){
                printf(FILE_NAME_ERROR_STR);
                printf(FAILURE_STR);
                close(client);
                return -1;
            }

            if(strcmp(argv[COMMAND_INDEX], CLIENT_UPLOAD) == 0){
                if(!send_file(client, argv[CLIENT_LOCAL_INDEX])){
                    printf(FAILURE_STR);
                    return -1;
                }

            }
            else if(strcmp(argv[COMMAND_INDEX], CLIENT_DOWNLOAD) == 0){

                if(!get_file(client, argv[CLIENT_LOCAL_INDEX])){
                    printf(FAILURE_STR);
                    return -1;
                }

            }
            printf(SUCCESS_STR);
            close(client);
        }
    }

}



