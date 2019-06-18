rm -rf dist/
python setup.py sdist
docker run -u $(id -u):$(id -g) -e PLAT=manylinux2010_x86_64 --mount type=bind,source=`pwd`,destination=/src quay.io/pypa/manylinux2010_x86_64 bash /src/build-manylinux.sh
docker run -u $(id -u):$(id -g) -e PLAT=manylinux1_x86_64 --mount type=bind,source=`pwd`,destination=/src quay.io/pypa/manylinux1_x86_64 bash /src/build-manylinux.sh
mv dist/manylinux1_x86_64/* dist/
rmdir dist/manylinux1_x86_64
mv dist/manylinux2010_x86_64/* dist/
rmdir dist/manylinux2010_x86_64
