from flask import request, abort
from ipaddress import IPv4Address, IPv4Network

NETWORKS = {
    IPv4Network("127.0.0.1"),
    IPv4Network("36.110.107.14"),
    IPv4Network("172.31.0.0/16"),
    IPv4Network("172.30.0.0/24"),
    IPv4Network("172.16.0.0/16"),
}


def access_control():
    client_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    client_ip = IPv4Address(client_ip)
    endpoint = request.endpoint
    for network in NETWORKS:
        if client_ip in network:
            break
    else:
        if not endpoint.startswith("forecast_api"):
            abort(403)
    return
