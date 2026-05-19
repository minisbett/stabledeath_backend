FROM alpine:3.22 AS runtime
ARG TARGETARCH
RUN apk add --no-cache ca-certificates sqlite-libs
WORKDIR /app
COPY dist/${TARGETARCH}/backend /usr/local/bin/backend
RUN mkdir -p /data && touch /data/timeseries.db && chmod 666 /data/timeseries.db
VOLUME /data
ENV APP_ADDR=0.0.0.0:6726
EXPOSE 6726
CMD ["backend"]
