import glob
import py_compile

files = glob.glob('**/*.py', recursive=True)
for path in files:
    try:
        py_compile.compile(path, doraise=True)
        print('OK', path)
    except py_compile.PyCompileError as e:
        print('FAIL', path)
        print(e)
        raise
print('compiled', len(files))
