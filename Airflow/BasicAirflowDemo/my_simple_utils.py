import os

for name, value in os.environ.items():
    if name.startswith("AIR"):
        print("{0}: {1}".format(name, value))