# Docker Port Mapping Explained

## Question: Why map 5434 → 5432? Can't we use 5434 directly?

**Short Answer**: No, we can't. Here's why:

---

## Docker Port Mapping Basics

Docker containers have their own internal network. Services inside containers listen on standard ports:
- **Postgres**: Always listens on port **5432** inside the container
- **MinIO API**: Always listens on port **9000** inside the container
- **MinIO Console**: Always listens on port **9001** inside the container

These are the **default ports** for these services and are baked into the Docker images.

## The Problem: Multiple Containers

You might want to run multiple Postgres instances:
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Project A      │     │  Project B      │     │  nse-momentum   │
│  Postgres       │     │  Postgres       │     │  Postgres       │
│  Container      │     │  Container      │     │  Container      │
│                 │     │                 │     │                 │
│  Listens on    │     │  Listens on    │     │  Listens on    │
│  port 5432   │     │  port 5432   │     │  port 5432   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
       │                        │                        │
       │                        │                        │
       ▼                        ▼                        ▼
   Host port?             Host port?              Host port?
   Must be unique!        Must be unique!         Must be unique!
```

All three containers use port 5432 **inside**, but on your host machine (your computer), you can only use each port once.

## The Solution: Port Mapping

Docker lets you map a **host port** to a **container port**:

```yaml
# docker-compose.yml
services:
  postgres:
    ports:
      - "127.0.0.1:5434:5432"
      #   ^HOST  ^CONTAINER
```

This means:
- **Inside container**: Postgres listens on 5432
- **On host machine**: You access it via 5434
- **Mapping**: `5434` → `5432`

## Connection String

When connecting from your host machine (Python app, psql, etc.):

```python
DATABASE_URL = "postgresql://user:pass@127.0.0.1:5434/db"
#                                                          ^^^^
#                                                     HOST PORT
```

## Can We Use 5434 Inside the Container?

**Option A: Use standard mapping (recommended) ✅**
```yaml
ports:
  - "127.0.0.1:5434:5432"  # Simple, standard
```

**Option B: Change Postgres config inside container (complex) ❌**
You'd have to:
1. Create a custom Postgres config file
2. Override the Postgres command to use it
3. Mount the config into the container
4. More maintenance, more complexity

**Verdict**: Option A is the standard Docker approach.

## Container-to-Container Communication

When containers talk to each other (like `minio-init` → `minio`), they use the **container port**:

```bash
# Inside minio-init container
curl http://minio:9000/health
#             ^SERVICE  ^CONTAINER_PORT
```

Note: They use `minio:9000`, NOT `minio:9003`. This is because:
- They're on the same Docker network
- They use the **internal** port (9000)
- Host port mapping (9003) only applies from outside Docker

## Our Port Configuration

| Service | Container Port (Internal) | Host Port (External) | Connection From |
|---------|--------------------------|---------------------|-----------------|
| Postgres | 5432 (fixed) | 5434 | Host machine (Python, etc.) |
| MinIO API | 9000 (fixed) | 9003 | Host machine (browser, etc.) |
| MinIO Console | 9001 (fixed) | 9004 | Host machine (browser) |

## Visual Summary

```
┌──────────────────────────────────────────────────────────────┐
│                     Your Host Machine                        │
│                                                              │
│  Python App connects to: 127.0.0.1:5434  ──────┐           │
│  Browser connects to:   127.0.0.1:9003  ─────┐ │           │
│                                         │  │  │           │
│                              ┌──────────┴──┴──▼─────────┐  │
│                              │    Docker Network        │  │
│                              │                          │  │
│                              │  ┌─────────────────┐     │  │
│                              │  │  Postgres       │     │  │
│                              │  │  Container      │     │  │
│                              │  │                 │     │  │
│                              │  │  Listens on    │     │  │
│                              │  │  INTERNAL 5432 │     │  │
│                              │  └─────────────────┘     │  │
│                              │                          │  │
│                              │  ┌─────────────────┐     │  │
│                              │  │  MinIO          │     │  │
│                              │  │  Container      │     │  │
│                              │  │                 │     │  │
│                              │  │  API: 9000     │     │  │
│                              │  │  Console: 9001 │     │  │
│                              │  └─────────────────┘     │  │
│                              │                          │  │
│                              └──────────────────────────┘  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Key Takeaways

1. **Container ports are fixed** (5432, 9000, 9001) - defined by Docker images
2. **Host ports are configurable** (5434, 9003, 9004) - you choose these
3. **Port mapping** `HOST:CONTAINER` lets you avoid conflicts
4. **Host apps use host ports** (5434, 9003, 9004)
5. **Container-to-container uses container ports** (5432, 9000, 9001)

## Why This is the Standard Approach

✅ **Simple**: No need to modify container configurations
✅ **Flexible**: Change host ports without changing containers
✅ **Safe**: Multiple projects can run simultaneously
✅ **Standard**: This is how Docker is designed to be used

## References

- Docker Compose port mapping: https://docs.docker.com/compose/compose-file/ports/
- Docker networking: https://docs.docker.com/network/
