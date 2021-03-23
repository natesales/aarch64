import ipaddress
from json import JSONEncoder, dumps
from secrets import token_hex

from argon2 import PasswordHasher
from bson import json_util
from fastapi import FastAPI, status, HTTPException
from pydantic import typing
from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError
from rich.console import Console
from rich.traceback import install
from starlette.responses import Response as StarletteResponse, RedirectResponse

import database
from models.admin import PoP, Host
from models.user import User, Project
from models.vm import VMRequest

install()  # Install rich traceback handler

VERSION = "0.0.1"

console = Console()
argon = PasswordHasher()

app = FastAPI(title="aarch64", version=VERSION)
db = database.get()
db["users"].create_index([("email", ASCENDING)], background=True, unique=True)
db["pops"].create_index([("name", ASCENDING)], background=True, unique=True)


class SafeJSONEncoder(JSONEncoder):
    """
    BSON-safe JSON response encoder
    """

    def default(self, obj):
        return json_util.default(obj)


class Response(StarletteResponse):
    """
    Response implements a starlette.responses.Response to encode BSON-safe JSON with SafeJSONEncoder
    """

    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            cls=SafeJSONEncoder,
        ).encode("utf-8")


@app.on_event("shutdown")
async def shutdown():
    db.close()


async def get_ips():
    """
    get_ips returns the next available IP address pair
    """

    # noinspection PyUnresolvedReferences
    config = await app.db["config"].find_one()
    if not config:
        raise Exception("Unable to find config document")

    for address in range(2, 254):  # Start at 2, as 1 is reserved for the control plane
        # noinspection PyUnresolvedReferences
        address_taken = await app.db["containers"].find_one({"ip4": config["prefix4"] + str(address)})
        if not address_taken:
            return config["prefix4"] + str(address), config["prefix6"] + str(address)


@app.get("/")
async def index():
    return RedirectResponse(url="/docs")


@app.post("/auth/signup")
async def signup(user: User):
    _user = user.dict()
    _user["api_key"] = str(token_hex(32))
    _user["password"] = argon.hash(_user["password"])
    try:
        new_user = await db["users"].insert_one(_user)
    except DuplicateKeyError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User with this email already exists")
    if new_user.inserted_id:
        return Response(status_code=status.HTTP_200_OK, content={"detail": "User created"})
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to create user")


@app.post("/project")
async def create_project(project: Project):
    new_project = await db["projects"].insert_one(project.dict())
    if new_project.inserted_id:
        return Response(status_code=status.HTTP_200_OK, content={"detail": f"Projected created"})

    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to create project")


@app.post("/auth/login")
async def login(user: User):
    user_doc = await db["users"].find_one({"email": user.email})
    if not user_doc or not argon.verify(user_doc["password"], user.password):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return Response(status_code=status.HTTP_200_OK, content=user_doc["api_key"])


@app.post("/admin/pop")
async def add_pop(pop: PoP):
    try:
        new_pop = await db["pops"].insert_one(pop.dict())
    except DuplicateKeyError:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="PoP with this name already exists")
    if new_pop.inserted_id:
        return Response(status_code=status.HTTP_200_OK, content={"detail": f"PoP {pop.name} added"})
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to create pop")


@app.post("/admin/host")
async def add_host(host: Host):
    # Cast IP types to string
    _host = host.dict()
    _host["ip"] = str(_host["ip"])

    # Get taken prefixes
    taken_prefixes = []
    async for pop in db["pops"].find():
        if pop.get("hosts"):
            for host in pop.get("hosts"):
                taken_prefixes.append(host["prefix"])

    # Find next available prefix
    config_doc = await db["config"].find_one()
    parent_prefix = ipaddress.ip_network(config_doc["prefix"])
    for slash48 in list(parent_prefix.subnets(new_prefix=48))[::-1]:
        slash48 = str(slash48)
        if slash48 not in taken_prefixes:
            _host["prefix"] = slash48
    if not _host.get("prefix"):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No available prefixes to assign")

    new_host = await db["pops"].update_one({"name": _host["pop"]}, {"$push": {"hosts": _host}})

    if new_host.matched_count == 1:
        return Response(status_code=status.HTTP_200_OK, content={"detail": f"Host added"})
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"PoP {host.pop} doesn't exist")


@app.get("/admin/ansible")
async def get_ansible_hosts():
    config_doc = await db["config"].find_one()
    _config = {
        "all": {
            "vars": {
                "ansible_user": config_doc["user"],
                "ansible_port": config_doc["port"],
                "ansible_ssh_private_key_file": config_doc["key_file"]
            },
            "hosts": {}
        }
    }

    async for pop in db["pops"].find():
        if pop.get("hosts"):
            for idx, host in enumerate(pop.get("hosts")):
                _config["all"]["hosts"][pop["name"] + str(idx)] = {
                    "ansible_host": host["ip"]
                }

    return Response(status_code=status.HTTP_200_OK, content=_config)


@app.post("/vms/create")
async def create_vm(vm: VMRequest):
    new_vm = await db["vms"].insert_one(vm.dict())
    if new_vm.inserted_id:
        return Response(status_code=status.HTTP_200_OK, content={"detail": f"VM added"})

    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to create VM")

# @app.post("/vms/create")
# async def create_vm(request: Request, container: Container):
#     _container = container.dict()
#
#     new_container = await request.app.db["containers"].insert_one(_container)
#     if not new_container.inserted_id:
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to create container")
#
#     _sanitized_container = _container
#     _sanitized_container["id"] = str(_container["_id"])
#     del _sanitized_container["_id"]
#
#     return Response(status_code=status.HTTP_201_CREATED, content=_sanitized_container)
# @app.get("/containers")
# async def get_containers(request: Request):
#     containers = []
#     async for container in request.app.db["containers"].find():
#         containers.append(container)
#
#     return Response(status_code=status.HTTP_200_OK, content=containers)
#
#
# @app.get("/containers/{container_id}")
# async def get_container(request: Request, container_id: str):
#     container = await request.app.db["containers"].find_one({"_id": database.to_object_id(container_id)})
#     if container:
#         return Response(status_code=status.HTTP_200_OK, content=container)
#     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Container not found")
#
#
#
# @app.put("/containers/{container_id}")
# async def update_container(request: Request, container_id: str, container: UpdateContainer):
#     modified_container = await request.app.db["containers"].update_one({"_id": database.to_object_id(container_id)}, {"$set": container.dict()})
#     if modified_container.matched_count != 1:
#         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Container not found")
#     elif modified_container.modified_count == 1:
#         return Response(status_code=status.HTTP_200_OK, content="Container modified")
#     else:
#         return Response(status_code=status.HTTP_200_OK, content="Container attributes are already the same, nothing to modify")
#
#
# @app.delete("/containers/{container_id}")
# async def delete_container(request: Request, container_id: str):
#     deleted_container = await request.app.db["containers"].delete_one({"_id": database.to_object_id(container_id)})
#     if deleted_container.deleted_count == 1:
#         return Response(status_code=status.HTTP_200_OK, content="Container deleted")
#     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Container not found")
#
#
# @app.put("/containers/{container_id}/pull")
# async def pull_container(request: Request, container_id: str):
#     # Toggle the container's pull attribute set to signal the cluster to pull it's image
#     await request.app.db["containers"].update_one({"_id": database.to_object_id(container_id)}, {"$set": {"pull": True}})
#     container = await request.app.db["containers"].update_one({"_id": database.to_object_id(container_id)}, {"$unset": {"pull": 1}})
#     if container.matched_count == 1:
#         return Response(status_code=status.HTTP_200_OK, content="Pulled new image")
#
#     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Container not found")
