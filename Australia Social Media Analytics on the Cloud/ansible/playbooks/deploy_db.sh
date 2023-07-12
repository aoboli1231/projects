#!/usr/bin/env bash
. ./group-15-openrc.sh; ansible-playbook --ask-become-pass deploy_db.yaml -i inventory/application_hosts.ini