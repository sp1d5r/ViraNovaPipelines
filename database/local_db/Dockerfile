version: '3'

services:
  db:
    image: postgres
    container_name: my_postgres_db
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DATABASE}
      POSTGRES_SSLMODE: ${POSTGRES_SSLMODE}
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
      - ./path/to/dump:/docker-entrypoint-initdb.d
    restart: unless-stopped

volumes:
  pg_data:
