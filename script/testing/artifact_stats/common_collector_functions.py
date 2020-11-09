import subprocess
from util.constants import LOG


def compile_binary(build_path, use_cache=True, is_debug=False):
    """ Compile binary 
    Args:
        build_path - The location of the binary
        use_cache - Whether to increase the speed by using the build cache
        is_debug - Whether to output the logs of the compilation to stdout
    Retrun:
        int - The exit code of the compilation (0=success)
    """
    cmake_cmd = f'cmake -GNinja -DNOISEPAGE_UNITY_BUILD=ON \
        {"-DCMAKE_CXX_COMPILER_LAUNCHER=ccache" if use_cache else ""} -DCMAKE_BUILD_TYPE=Release \
        -DNOISEPAGE_USE_ASAN=OFF -DNOISEPAGE_USE_JEMALLOC=ON ..'
    ninja_cmd = 'ninja noisepage'
    compile_commands = [cmake_cmd, ninja_cmd]

    try:
        for cmd in compile_commands:
            subprocess.run(cmd, cwd=build_path, shell=True, check=True, capture_output=(not is_debug))
    except subprocess.CalledProcessError as err:
        LOG.debug(err.stdout)
        LOG.error(err.stderr)
        return err.returncode

    return 0
