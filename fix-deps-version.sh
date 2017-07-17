find deps/erldn/ -name '*app*' -exec sed -i s/\`cat\ VERSION\`/1.0.0/ {} \;
rm _build -rf
mix deps.compile erldn