#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>
#include <pthread.h>

#include <iostream>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <mqtt/async_client.h>
#define MAXLINE 4096

const std::string SERVER_ADDRESS2("tcp://www.neptune-iiot.net:9883");
const std::string PASSWORD2("8357-1E3F-EFB1-D389");
const std::string CLIENT_ID2("c68fcd9688d9150b1d5b142309224a2b");
const std::string TOPIC22("devicesub");
const std::string TOPIC222("devicepub");
char message_2[1024];


const std::string SERVER_ADDRESS("tcp://61.238.107.202:1883");
const std::string CLIENT_ID("user_20251104");
const std::string TOPIC("222226/20251104/led_ctl");
const std::string TOPIC2("222226/20251104/sensor");
int socket_fd;
bool init_mqtt;
constexpr int BUFFER_SIZE = 1024;
bool connect_break =false;
struct sockaddr_in addr;
using namespace std;
bool message_updata;
char message[1024];
char old_message[1024];
char co2;
char ec;
char pH;
/////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////// setting the parm
////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
bool isLocalhostReachable() {
   usleep(100000);
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Socket creation failed." << std::endl;
        return false;
    }
    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_port = htons(8081); // Common HTTP port
    server.sin_addr.s_addr = inet_addr("192.168.1.103");
    // Try to connect to the localhost
    if (connect(sock, (struct sockaddr*)&server, sizeof(server)) < 0) {
        close(sock);
        return false; // Localhost is not reachable 
        }
    close(sock);
    return true; // Localhost is reachable
}

////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////// ping ip
////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
bool ping_internet()
{
   int result = std::system("ping -c 3 www.google.com > /dev/null 2>&1"); 
    if (result == 0) {
        std::cout << "Google is reachable." << std::endl;
    } else {
        std::cout << "Google is unreachable." << std::endl;
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////// tcp iniy
////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
int tcp_init()
{
    //socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    while (true)
    {
     if (isLocalhostReachable()) {
         std::cout << "Localhost is reachable." << std::endl;
        cout << "socket 创建失败：" << endl;
        socket_fd = socket(AF_INET, SOCK_STREAM, 0);
        }
        break;
        if (socket_fd != -1)
        {
            break;
        }
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8081);
    addr.sin_addr.s_addr = inet_addr("192.168.1.103");
    int res = connect(socket_fd, (struct sockaddr *)&addr, sizeof(addr));
    if (res == -1)
    {
        cout << "bind 链接失败：" << endl;
            tcp_init();
    }
    cout << "bind 链接成功：" << endl;
}

class callback : public virtual mqtt::callback, public virtual mqtt::iaction_listener
{
private:
    mqtt::async_client &client_;
public:
    callback(mqtt::async_client &client) : client_(client) {}
    void connection_lost(const std::string &cause) override
    {
        connect_break = true;
        std::cout << "\nConnection lost" << std::endl;
        if (!cause.empty())
            std::cout << "\tcause: " << cause << std::endl;
    }
      void delivery_complete(mqtt::delivery_token_ptr token) override {}
    void message_arrived(const mqtt::const_message_ptr msg) override
    {
        std::cout << "Message arrived: " << msg->get_payload_str() << std::endl;
        string s1 = msg->get_payload_str();
        char *data;
        int len = s1.length();
        data = (char *)malloc((len + 1) * sizeof(char));
        s1.copy(data, len, 0);
        write(socket_fd, data, strlen(data));
    }

    void on_failure(const mqtt::token &tok) override
    {
        std::cout << "Connection attempt failed" << std::endl;
    }

    void on_success(const mqtt::token &tok) override
      {
        std::cout << "Connection attempt successful" << std::endl;
        cout << TOPIC << endl;
        mqtt::message_ptr pubmsg = mqtt::make_message(TOPIC, "Hello from Paho C++");
        pubmsg->set_qos(1);
        client_.publish(pubmsg);
    }
};
void* connect_tcp(void* arg)
{
        //Check if Location host is reachable
        std::cout << "Localhost is reachable." << std::endl;
        tcp_init();
        auto start =std::chrono::system_clock::now();
        while (true)
        {
        char buffer[BUFFER_SIZE] = {0};
        std::string hello = "{\"odj\":\"serverstate\"}";
        auto end =std::chrono::system_clock::now();
        message_updata =false;
        std::chrono::duration<double> elapsed_seconds = end -start;
        std::time_t end_time = std::chrono::system_clock::to_time_t(end);
        if(elapsed_seconds.count() > std::chrono::duration<double>(1).count())
        {
        std::cout << "\nSending next message..." << std::endl;
        write(socket_fd, hello.c_str(), hello.size());
        std::cout << "{\"odj\":\"serverstate\"}" << std::endl;
        ssize_t valread = read(socket_fd, message, BUFFER_SIZE);
        std::cout << "Received: " << message << std::endl;
        std::cout<< "len"<< strlen(message)<<std::endl;
        if (strlen(message) >0)
        {
      strcpy(old_message,message);
        }
         //std::cout << "Received: " << valread << std::endl;
           if (valread == 0) {
            std::cout << "Server closed connection. Disconnecting." << std::endl;
            close(socket_fd);
            socket_fd = -1;
           } else if (valread == -1)
           {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
             {
                // Actual error other than no data available right now
                std::cerr << "Recv failed: " << strerror(errno) << ". Disconnecting." << std::endl;
                close(socket_fd);
                socket_fd = -1;
                usleep(10000000);
                tcp_init();
             }
        } else
{
            // Data received, process it
            // ...
        }
        start = end;
        usleep(1000);
    }
}}

//uqdata 13/4/2026


void* connect_mqtt_2(void* arg)
{
while (true){
    mqtt::async_client client(SERVER_ADDRESS2,PASSWORD2);
    callback cb(client); 
    client.set_callback(cb);
    mqtt::connect_options connOpts;
    connOpts.set_clean_session(true);
    connOpts.set_user_name(CLIENT_ID2);
    connOpts.set_password(PASSWORD2);
    connOpts.set_keep_alive_interval(20);
    try
    {
        std::cout << "Connecting to the MQTT server..." << std::endl;
        mqtt::token_ptr conntok = client.connect(connOpts);
        std::cout << "Connecting to the MQTT server..." << std::endl;
        conntok->wait();
        std::cout << "Connected!" << std::endl;
        // 订阅消息
       client.subscribe(TOPIC22, 1);
       auto start =std::chrono::system_clock::now();
       while (true)
       {
       if(!client.is_connected())
       {
  break;
       }
//        if(message_updata)
       {
        auto end =std::chrono::system_clock::now();
        message_updata =false;
        std::chrono::duration<double> elapsed_seconds = end -start;
        std::time_t end_time = std::chrono::system_clock::to_time_t(end);
//        const char* source =  "{\"action\": \"data\", \"uuid\":\"8357-1E3F-EFB1-D389\",\"data\":\"senor_001\": 146,\"senor_$
string source = std::string( "{\"action\": \"data\", \"uuid\":\"8357-1E3F-EFB1-D389\",") +std::string(old_message) + "}";
        strcpy(message_2, source.c_str());
//        std::cout << "\nSending next message..." << std::endl;
       client.publish(TOPIC222, message_2,strlen(message_2)+1 );
//         client.publish(TOPIC222,source,120);

        std::cout<<message_2<<std::endl;
//        std::cout << message << std::endl;
//       std::cout << elapsed_seconds.count()<<std::endl;
      start = end;
       }
  usleep(1000000);
       }
         }
    catch (const mqtt::exception &exc)
    {
        std::cerr << "Error: " << exc.what() << std::endl;
       // return 1;
    }
}
}


void* connect_mqtt(void* arg)
{
    while (true){
    mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
    callback cb(client);
    client.set_callback(cb);
    mqtt::connect_options connOpts;
    connOpts.set_clean_session(true);
   init_mqtt =false;
    try
    {
        mqtt::token_ptr conntok = client.connect(connOpts);
        conntok->wait();
        client.subscribe(TOPIC, 1);
       auto start =std::chrono::system_clock::now();
       while (true)   
       {
        if (init_mqtt ==true){ 
        mqtt::async_client client(SERVER_ADDRESS, CLIENT_ID);
        callback cb(client);
        client.set_callback(cb);
        mqtt::connect_options connOpts;
        connOpts.set_clean_session(true);
        try{
        std::cout << "Connecting to the MQTT server..." << std::endl;
        mqtt::token_ptr conntok = client.connect(connOpts);
        conntok->wait();
        client.subscribe(TOPIC, 1);
        init_mqtt = false;
         }catch (const mqtt::exception &exc)
       {
        std::cerr << "Error: " << exc.what() << std::endl;
       break;
       // return 1;
       }  }
       if(!client.is_connected())
       {
       break;
//     init_mqtt =true;
       }
//        if(message_updata) 
      {
        auto end =std::chrono::system_clock::now();
        message_updata =false;
        std::chrono::duration<double> elapsed_seconds = end -start;
        std::time_t end_time = std::chrono::system_clock::to_time_t(end);
        if(elapsed_seconds.count() > std::chrono::duration<double>(1).count())
        {
//        std::cout << "\nSending next message..." << std::endl;
//        client.publish(TOPIC2, message, strlen(message)+1);
//        std::cout<<"message mqtt"<<std::endl;
//      std::cout << message << std::endl;
        start = end;
        }
        }
        usleep(1000000);
        }  
        }
    catch (const mqtt::exception &exc)
    {
        std::cerr << "Error: " << exc.what() << std::endl;
       // return 1;
    }
 }
}
int main(int argc, char *argv[])
{

    pthread_t thread1, thread2, thread3;
    // Create the first task thread
    if (pthread_create(&thread1, nullptr, connect_tcp, nullptr) != 0 && isLocalhostReachable() == false) {
        std::cerr << "Error creating thread 1" << std::endl;
        return 1;
   }
   if (pthread_create(&thread2, nullptr, connect_mqtt, nullptr) != 0 ) {
        std::cerr << "Error creating thread 1" << std::endl;
        return 1;
    }
//   if (pthread_create(&thread3, nullptr, connect_mqtt_2, nullptr) != 0 ) {
//        std::cerr << "Error creating thread 1" << std::endl;
//        return 1;
//    }

    // Wait for both threads to complete
    pthread_join(thread1, nullptr);
    pthread_join(thread2, nullptr);
  //  pthread_join(thread3, nullptr); 
}

