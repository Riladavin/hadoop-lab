import os
for datanodes in [1, 3]:
    for optimized in ["True", "False"]:
        os.system(f"./run.sh --datanodes {datanodes} --optimized {optimized}")