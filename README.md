# input-service

![input-service](need_input.jpg)

## what is this? 
The `input-service` is the secure go-between between the outside world sending event data to PatchFox. It serves the following functions:

* is the only ingress point into PatchFox
* determines whether or not an event message is properly formed
* handles datasource initialization in a manner that ensures historical data is processed in ascending temporal order

## how do I make it do stuff on my workstation? 
1. From project root, invoke `docker-compose up` (or `docker compose up` depending on what your OS and docker setup is.) This will start kafka, zookeeper, and postgres. 

2. In a new terminal, and from project root, invoke `mvn spring-boot:run`. This will build the project and spin up the service. 

## what endpoints are exposed by this service? 
* with the service up and running navigate your browser to `http://localhost:8080/swagger-ui/index.html` which will list the endpoints, what they do, and how to invoke them. 

## where are the patchfox json entities located for common payloads? 
see project [package-utils](https://gitlab.com/patchfox2/package-utils). It has all the goodies you seek. 

## where are the patchfox JPA entities located?
see project [db-utils](https://gitlab.com/patchfox2/db-utils). It has all the goodies you seek. 

## where can I get more information? 
This is a PatchFox [turbo](https://gitlab.com/patchfox2/turbo) service. Click the link for more information on what that means and what turbo-charged services provide to both developers and consumers. 
