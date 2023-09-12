#!/bin/bash

# Colors
RESET='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
LIGHTCYAN='\033[1;36m'

# Docker required version variables
REQUIRED_DOCKER_VERSION='20.10'
REQUIRED_DOCKER_COMPOSE_VERSION='2.3.4'

# Checking if docker and docker-compose exists
if ! command -v docker --version &> /dev/null
then
    echo -e "${RED}docker${RESET} could not be found. ${LIGHTCYAN}Please install version >= ${REQUIRED_DOCKER_VERSION}${RESET}"
    exit
fi

if ! command -v docker-compose --version &> /dev/null
then
    echo -e "${RED}docker-compose${RESET} could not be found. ${LIGHTCYAN}Please install version >= ${REQUIRED_DOCKER_COMPOSE_VERSION}${RESET}"
    exit
fi

# Docker current version variables
CURRENT_DOCKER_VERSION="$(docker version --format '{{.Server.Version}}')"
CURRENT_DOCKER_COMPOSE_VERSION="$(docker-compose version --short)"

# Version check function "greater than or equal to"
function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

# Check docker and docker-compose versions
if ! version_ge $CURRENT_DOCKER_VERSION $REQUIRED_DOCKER_VERSION; then
   echo -e "${RED}Please upgrade docker!${RESET}"
   echo -e "Your docker version is: ${RED}${CURRENT_DOCKER_VERSION}${RESET}. Required version is ${GREEN}>= ${REQUIRED_DOCKER_VERSION}${RESET}"
fi

if ! version_ge $CURRENT_DOCKER_COMPOSE_VERSION $REQUIRED_DOCKER_COMPOSE_VERSION; then
   echo -e "${RED}Please upgrade docker-compose!${RESET}"
   echo -e "Your docker-compose version is: ${RED}${CURRENT_DOCKER_COMPOSE_VERSION}${RESET}. Required version is ${GREEN}>= ${REQUIRED_DOCKER_COMPOSE_VERSION}${RESET}"
fi

echo -e "${GREEN}Cattr for Docker installer"
echo -e "${RESET}We will ask you a few questions to configure your shiny new Cattr instance\n"

echo -e "\nStarting installation\n"

echo -e "\nDownloading docker-compose.yml"

wget https://git.amazingcat.net/cattr/core/docker/-/releases/permalink/latest/downloads/compose -O ./docker-compose.yml

echo -e "\nTrying to run docker compose"

docker compose -p cattr up -d

echo -e "\n${GREEN}Docker container was deployed.${RESET}"
docker compose -p cattr run --rm -it backend install

# Exit
echo -e "\n${GREEN}Installation is done, thank you!${RESET}"
