cd /src/
mkdir ./dist/$PLAT
for PYBIN in /opt/python/cp36-cp36m/bin /opt/python/cp37-cp37m/bin; do
  $PYBIN/python setup.py bdist_wheel
done
for whl in dist/*.whl; do
    auditwheel repair --plat=$PLAT "$whl" -w ./dist/$PLAT
    rm "$whl"
done
