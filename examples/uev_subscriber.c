
/**
 * @file
 * A simple program that subscribes to a topic.
 */
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include <uev/uev.h>

#include <mqtt.h>
#include "templates/posix_sockets.h"


/**
 * @brief The function will be called whenever a PUBLISH message is received.
 */
void publish_callback(void** unused, struct mqtt_response_publish *published);

/**
 * @brief Safelty closes the \p sockfd and cancels the \p client_daemon before \c exit. 
 */
void exit_example(int status, int sockfd);

static void sockfd_cb(uev_t *w, void *arg, int events) {
    struct mqtt_client *client = arg;

    if (events & UEV_ERROR) {
        fprintf(stderr, "sockfd error\n");
    }

    if (events & UEV_READ) {
        //fprintf(stderr, "data available\n");
        mqtt_notify_recv(client);
    }

    if (events & UEV_WRITE) {
        //fprintf(stderr, "ready to write\n");
        mqtt_notify_send(client);
    }
}

static void pingtimer_cb(uev_t *w, void *arg, int events) {
    struct mqtt_client *client = arg;

    if (events & UEV_ERROR) {
        fprintf(stderr, "pingtimer error\n");
    }

    fprintf(stderr, "ping timeout\n");
    mqtt_notify_pingtimer(client);
}

static void acktimer_cb(uev_t *w, void *arg, int events) {
    struct mqtt_client *client = arg;

    if (events & UEV_ERROR) {
        fprintf(stderr, "acktimer error\n");
    }

    fprintf(stderr, "ack timeout\n");
    mqtt_notify_acktimer(client);
}

static uev_t w_pingtimer;
static uev_t w_acktimer;
static uev_t w_sockfd;

static void set_ping_timer(struct mqtt_client *client, mqtt_pal_time_t t) {
    int timeout = t ? (t - MQTT_PAL_TIME()) * 1000 : 0;
    //fprintf(stderr, "%s(%llu -> %d)\n", __func__, (unsigned long long)t, timeout);
    uev_timer_set(&w_pingtimer, timeout, 0);
}

static void set_ack_timeout(struct mqtt_client *client, mqtt_pal_time_t t) {
    int timeout = t ? (t - MQTT_PAL_TIME()) * 1000 : 0;
    //fprintf(stderr, "%s(%llu -> %d)\n", __func__, (unsigned long long)t, timeout);
    uev_timer_set(&w_pingtimer, timeout, 0);
}

static void enable_sendready_event(struct mqtt_client *client, int enabled) {
    int events = UEV_READ | UEV_ERROR;
    //fprintf(stderr, "%s(%d)\n", __func__, enabled);

    if (enabled)
        events |= UEV_WRITE;

    uev_io_set(&w_sockfd, w_sockfd.fd, events);
}

int main(int argc, const char *argv[]) 
{
    int rc;
    const char* addr;
    const char* port;
    const char* topic;

    /* get address (argv[1] if present) */
    if (argc > 1) {
        addr = argv[1];
    } else {
        addr = "test.mosquitto.org";
    }

    /* get port number (argv[2] if present) */
    if (argc > 2) {
        port = argv[2];
    } else {
        port = "1883";
    }

    /* get the topic name to publish */
    if (argc > 3) {
        topic = argv[3];
    } else {
        topic = "datetime";
    }

    uev_ctx_t uev;
    if (uev_init(&uev)) {
        fprintf(stderr, "Failed to init uev\n");
        exit_example(EXIT_FAILURE, -1);
    }

    /* open the non-blocking TCP socket (connecting to the broker) */
    int sockfd = open_nb_socket(addr, port);

    if (sockfd == -1) {
        perror("Failed to open socket: ");
        exit_example(EXIT_FAILURE, sockfd);
    }

    /* setup a client */
    struct mqtt_client client;
    uint8_t sendbuf[2048]; /* sendbuf should be large enough to hold multiple whole mqtt messages */
    uint8_t recvbuf[1024]; /* recvbuf should be large enough any whole mqtt message expected to be received */
    mqtt_init(&client, sockfd, sendbuf, sizeof(sendbuf), recvbuf, sizeof(recvbuf), publish_callback);
    client.set_ping_timer = set_ping_timer;
    client.set_ack_timeout = set_ack_timeout;
    client.enable_sendready_event = enable_sendready_event;

    mqtt_connect(&client, "subscribing_client", NULL, NULL, 0, NULL, NULL, 0, 400);

    /* check that we don't have any errors */
    if (client.error != MQTT_OK) {
        fprintf(stderr, "error: %s\n", mqtt_error_str(client.error));
        exit_example(EXIT_FAILURE, sockfd);
    }

    if (uev_io_init(&uev, &w_sockfd, sockfd_cb, &client, sockfd, UEV_READ | UEV_ERROR)) {
        fprintf(stderr, "uev_io_init failed\n");
        exit_example(EXIT_FAILURE, sockfd);
    }

    if (uev_timer_init(&uev, &w_pingtimer, pingtimer_cb, &client, 0, 0)) {
        fprintf(stderr, "uev_timer_init failed\n");
        exit_example(EXIT_FAILURE, sockfd);
    }

    if (uev_timer_init(&uev, &w_acktimer, acktimer_cb, &client, 0, 0)) {
        fprintf(stderr, "uev_timer_init failed\n");
        exit_example(EXIT_FAILURE, sockfd);
    }

    /* subscribe */
    mqtt_subscribe(&client, topic, 0);

    /* start publishing the time */
    printf("%s listening for '%s' messages.\n", argv[0], topic);
    printf("Press CTRL-C to exit.\n\n");
    
    rc = uev_run(&uev, 0);
    if (rc) {
        fprintf(stderr, "uev_run returned %d\n", rc);
    }
    
    /* disconnect */
    printf("\n%s disconnecting from %s\n", argv[0], addr);
    sleep(1);

    /* exit */ 
    exit_example(EXIT_SUCCESS, sockfd);
}

void exit_example(int status, int sockfd)
{
    if (sockfd != -1) close(sockfd);
    exit(status);
}



void publish_callback(void** unused, struct mqtt_response_publish *published) 
{
    /* note that published->topic_name is NOT null-terminated (here we'll change it to a c-string) */
    char* topic_name = (char*) malloc(published->topic_name_size + 1);
    memcpy(topic_name, published->topic_name, published->topic_name_size);
    topic_name[published->topic_name_size] = '\0';

    printf("Received publish('%s'): %.*s\n", topic_name, (int)published->application_message_size, (const char*) published->application_message);

    free(topic_name);
}
