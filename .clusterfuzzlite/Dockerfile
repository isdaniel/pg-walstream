FROM gcr.io/oss-fuzz-base/base-builder-rust
COPY . $SRC/pg-walstream
WORKDIR $SRC/pg-walstream
COPY ./.clusterfuzzlite/build.sh $SRC/
