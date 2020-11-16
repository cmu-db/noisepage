# Docker

## Installation

### Getting Docker
#### Mac (Homebrew)

1. `brew cask install docker`
2. Launch /Applications/Docker.app.

#### Other (older Ubuntu versions)

See  [Install Docker CE](https://docs.docker.com/install/linux/docker-ce/ubuntu/#set-up-the-repository). The recommended approach is to install using the repository. See section "Install Docker CE", subsection "Install using the repository" and follow the instructions for:

* SET UP THE REPOSITORY followed by 
* INSTALL DOCKER CE

See [Docker CE](https://www.docker.com/community-edition) if additional information is required.

### Setup

1. Launch Docker.
2. From the folder containing the Dockerfile, build the Docker image.
    - `docker build -t cmu-db/noisepage .`
    - docker will load your local repo into a `/repo` directory in the image

## Usage

1. Run the Docker image: `docker run -itd --name build cmu-db/noisepage`
3. Run CMake:
  - `docker exec build cmake ..`
  - `docker exec build make`
  - `docker exec build make unittest`

You can interact with the Docker image with
- single commands: `docker exec [-w WORKING_DIRECTORY] build BASH_COMMAND`
- interactive: `docker exec -it build bash`
- by default, the docker image starts in the `/repo/build` directory


**Note: The below step DELETES all the work you have on Docker.**

To stop the Docker image, run both:
1. `docker container stop build`
2. `docker rm build`


## Quirks

Docker on Windows and Docker on Mac do not behave nicely with LSAN. LSAN needs to be allowed to spawn a ptrace thread. You'll need to `docker run --cap-add SYS_PTRACE ...` to get it working.