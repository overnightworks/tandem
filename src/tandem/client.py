"""The Redis client type tandem takes, so a host types its parameter without importing redis."""

from redis.asyncio import Redis

type Client = Redis
