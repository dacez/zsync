#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/event.h>

#define PORT 12345
#define BACKLOG 5
#define BUFFER_SIZE 1024

int main() {
    int server_socket, client_socket;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    struct kevent events[BACKLOG];
    int kq, nev;

    // Create server socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket == -1) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Set socket options
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        perror("Setsockopt failed");
        exit(EXIT_FAILURE);
    }

    // Initialize server address struct
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    // Bind socket
    if (bind(server_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for connections
    if (listen(server_socket, BACKLOG) == -1) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    // Create kqueue
    kq = kqueue();
    if (kq == -1) {
        perror("Kqueue creation failed");
        exit(EXIT_FAILURE);
    }

    // Register server socket for accept events
    struct kevent ev;
    EV_SET(&ev, server_socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
    if (kevent(kq, &ev, 1, NULL, 0, NULL) == -1) {
        perror("Registering server socket for accept events failed");
        exit(EXIT_FAILURE);
    }

    printf("Server started on port %d\n", PORT);

    // Event loop
    while (1) {
        nev = kevent(kq, NULL, 0, events, BACKLOG, NULL);
        if (nev == -1) {
            perror("Kevent failed");
            exit(EXIT_FAILURE);
        }

        for (int i = 0; i < nev; ++i) {
            if (events[i].ident == server_socket) {
                // Accept new connection
                client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &client_addr_len);
                if (client_socket == -1) {
                    perror("Accept failed");
                    exit(EXIT_FAILURE);
                }
                printf("New connection from %s:%d\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

                // Register client socket for read events
                EV_SET(&ev, client_socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
                if (kevent(kq, &ev, 1, NULL, 0, NULL) == -1) {
                    perror("Registering client socket for read events failed");
                    exit(EXIT_FAILURE);
                }
            } else {
                // Handle data from client
                client_socket = events[i].ident;
                char buffer[BUFFER_SIZE];
                ssize_t bytes_received = recv(client_socket, buffer, BUFFER_SIZE, 0);
                if (bytes_received == -1) {
                    perror("Receive failed");
                    exit(EXIT_FAILURE);
                } else if (bytes_received == 0) {
                    // Connection closed
                    printf("Connection closed by %s:%d\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
                    close(client_socket);
                } else {
                    // Process received data (optional)
                    printf("Received data from %s:%d: %s\n", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port), buffer);
                    // Echo data back to client (optional)
                    send(client_socket, buffer, bytes_received, 0);
                }
            }
        }
    }

    // Close server socket
    close(server_socket);

    return 0;
}
