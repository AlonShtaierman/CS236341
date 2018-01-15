import socket as Socket
from socket import socket
import sys
import threading
import time


class ServerSession(threading.Thread):

    def __init__(self, clientSocket, serverSocket, request):
        threading.Thread.__init__(self)
        self.clientSocket = clientSocket
        self.serverSocket = serverSocket
        self.message = request
        self.bufferSize = 1024

    def run(self):
        self.serverSocket.send(self.message)            #LB sends request to the server
        res = self.serverSocket.recv(self.bufferSize)   #LB receives result from the server
        self.clientSocket.send(res)                     #LB routes result to client
        self.clientSocket.close()                       #LB releases the current client socket
        return


class LoadBalancer():

    def __init__(self, initIp, initPort, serverList):
        self.ip = initIp
        self.port = initPort
        self.knownServerList = serverList
        self.serverConnections = {}
        self.clientSocket = None
        self.bufferSize = 1024
        self.runningSessions = []
        self.timestamp = 0
        self.serverSchedule = {}
        for serverNum in self.knownServerList:
            self.serverSchedule[serverNum] = 0

    def loadBalance(self):
        print("starting run...")
        for serverNum,ip in self.knownServerList.items():                   #establishes a connection from the LB
            serverConnection = socket(Socket.AF_INET, Socket.SOCK_STREAM)   #to each known server
            serverConnection.connect((ip, self.port))
            print("lb connected to " + ip)
            self.serverConnections[serverNum] = serverConnection
        self.clientSocket = socket(Socket.AF_INET, Socket.SOCK_STREAM)      #Opens a socket for hosts to communicate
        self.clientSocket.bind((self.ip, self.port))                        #with the LB
        self.clientSocket.listen(5)

        while True:                                                         #Intercepts client messages
            newClient, clientIp = self.clientSocket.accept()
            request = newClient.recv(self.bufferSize)
            print("Message received from client IP " + clientIp[0])
            print("     Message is: " + request)
            delegatedServer = self.greedyBalance(request)                   #This finds the best server, via our alg.
            newSession = ServerSession(newClient, delegatedServer, request) #Each request gets its dedicated session
            self.runningSessions.append(newSession)
            newSession.start()

    def greedyBalance(self, request):
        currentTime = time.clock()
        timePassed = currentTime - self.timestamp                           #How long since last route was made
        for serverNum,workTime in self.serverSchedule.items():
            currentWorkTime = max(workTime - timePassed, 0)                 #Calculating how much work time each server has left
            self.serverSchedule[serverNum] = currentWorkTime

        self.timestamp = currentTime

        weightedProjection = self.getWeightedResponseTime(request)
        greedyServer = 0
        minimalResponseTime = weightedProjection[greedyServer]
        for serverNum, responseTime in weightedProjection.items():          #Finding the server which minimizes reponse time
            if responseTime < minimalResponseTime:
                greedyServer = serverNum
                minimalResponseTime = responseTime

        self.serverSchedule[greedyServer] = minimalResponseTime             #Updating the selected server's workload accordingly
        return self.serverConnections[greedyServer]                         #returning best server's socket

    def getWeightedResponseTime(self, request):                             #produces a vector of potential response times
        serverWeights = {"M": [2,2,1], "V": [1,1,3], "P": [1,1,2]}          #based on a given request
        type = request[0]
        time = int(request[1])
        expectedResponseTimes = {}
        for serverNum,workTime in self.serverSchedule.items():
            expectedResponseTimes[serverNum] = workTime + time*serverWeights[type][serverNum]
        return expectedResponseTimes



if __name__ == '__main__':
    print("Starting loadBalancer.py...")
    knowServers = {0:'192.168.0.101', 1:'192.168.0.102', 2:'192.168.0.103'}
    localIp = '10.0.0.1'
    localPort = 80
    LB = LoadBalancer(localIp, localPort, knowServers)
    LB.loadBalance()


