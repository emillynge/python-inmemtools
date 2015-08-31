__author__ = 'emil'
import re
import zipfile
from collections import defaultdict
import json
import imp
import sys

class BytesIO(object):
    """class StringIO([buffer])

    When a StringIO object is created, it can be initialized to an existing
    string by passing the string to the constructor. If no string is given,
    the StringIO will start empty.

    The StringIO object can accept either Unicode or 8-bit strings, but
    mixing the two may take some care. If both are used, 8-bit strings that
    cannot be interpreted as 7-bit ASCII (that use the 8th bit) will cause
    a UnicodeError to be raised when getvalue() is called.
    """
    def __init__(self, buf = '', mode='r', encoding='utf8'):
        # Force self.buf to be a string or unicode
        if isinstance(buf, bytearray):
            self.buf = buf
        else:
            self.buf = bytearray(self._encode(buf))

        self.len = len(self.buf)
        self.pipeline = bytearray()
        self.encoding = encoding
        if mode in ['w+', 'a', 'a+']:
            self.pos = self.len
        else:
            self.pos = 0
        self._write_pos = self.pos
        self.closed = False
        self.softspace = 0

    def __iter__(self):
        return self

    def next(self):
        """A file object is its own iterator, for example iter(f) returns f
        (unless f is closed). When a file is used as an iterator, typically
        in a for loop (for example, for line in f: print line), the next()
        method is called repeatedly. This method returns the next input line,
        or raises StopIteration when EOF is hit.
        """
        _complain_ifclosed(self.closed)
        r = self.readline()
        if not r:
            raise StopIteration
        return r

    @staticmethod
    def _encode(s):
        if isinstance(s, unicode):
            return s.encode()
        elif isinstance(s, basestring):
            return s
        else:
            return str(s)

    def close(self):
        """Free the memory buffer.
        """
        if not self.closed:
            self.flush()
            self.closed = True
            del self.pos

    def isatty(self):
        """Returns False because StringIO objects are not connected to a
        tty-like device.
        """
        _complain_ifclosed(self.closed)
        return False

    def seek(self, pos, mode = 0):
        """Set the file's current position.

        The mode argument is optional and defaults to 0 (absolute file
        positioning); other values are 1 (seek relative to the current
        position) and 2 (seek relative to the file's end).

        There is no return value.
        """
        _complain_ifclosed(self.closed)
        if self.pipeline:
            self.flush()
        if mode == 1:
            pos += self.pos
        elif mode == 2:
            pos += self.len
        self.pos = max(0, pos)

    def tell(self):
        """Return the file's current position."""
        _complain_ifclosed(self.closed)
        return self.pos

    def read(self, n = -1):
        """Read at most size bytes from the file
        (less if the read hits EOF before obtaining size bytes).

        If the size argument is negative or omitted, read all data until EOF
        is reached. The bytes are returned as a string object. An empty
        string is returned when EOF is encountered immediately.
        """
        _complain_ifclosed(self.closed)
        if self.pipeline:
            self.flush()
        if n is None or n < 0:
            newpos = self.len
        else:
            newpos = min(self.pos+n, self.len)
        r = self.buf[self.pos:newpos]
        self.pos = newpos
        return r.decode(self.encoding)

    def readline(self, length=None):
        r"""Read one entire line from the file.

        A trailing newline character is kept in the string (but may be absent
        when a file ends with an incomplete line). If the size argument is
        present and non-negative, it is a maximum byte count (including the
        trailing newline) and an incomplete line may be returned.

        An empty string is returned only when EOF is encountered immediately.

        Note: Unlike stdio's fgets(), the returned string contains null
        characters ('\0') if they occurred in the input.
        """
        _complain_ifclosed(self.closed)
        if self.pipeline:
            self.flush()
        i = self.buf.find('\n', self.pos)
        if i < 0:
            newpos = self.len
        else:
            newpos = i+1
        if length is not None and length >= 0:
            if self.pos + length < newpos:
                newpos = self.pos + length
        r = self.buf[self.pos:newpos]
        self.pos = newpos
        return r.decode(self.encoding)

    def readlines(self, sizehint = 0):
        """Read until EOF using readline() and return a list containing the
        lines thus read.

        If the optional sizehint argument is present, instead of reading up
        to EOF, whole lines totalling approximately sizehint bytes (or more
        to accommodate a final whole line).
        """
        total = 0
        lines = []
        line = self.readline()
        while line:
            lines.append(line)
            total += len(line)
            if 0 < sizehint <= total:
                break
            line = self.readline()
        return lines

    def truncate(self, size=None):
        """Truncate the file's size.

        If the optional size argument is present, the file is truncated to
        (at most) that size. The size defaults to the current position.
        The current file position is not changed unless the position
        is beyond the new file size.

        If the specified size exceeds the file's current size, the
        file remains unchanged.
        """
        _complain_ifclosed(self.closed)
        if size is None:
            size = self.pos
        elif size < 0:
            raise IOError(EINVAL, "Negative size not allowed")
        elif size < self.pos:
            self.pos = size
        del self.buf[size:]
        self.len = size

    def write(self, s):
        """Write a string to the file.

        There is no return value.
        """
        _complain_ifclosed(self.closed)
        if not s: return
        # Force s to be a string or unicode
        s = self._encode(s)

        spos = self.pos
        slen = self.len
        if spos == slen:
            self.pipeline += s
            self.len = self.pos = spos + len(s)
            return

        if spos > slen:
            self.pipeline += '\0'*(spos - slen)
            slen = spos
        newpos = spos + len(s)

        if spos < slen:
            if self.pipeline:
                self.flush()
                self._write_pos = spos
            self.pipeline += s
            if newpos > slen:
                slen = newpos
        else:
            self.pipeline += s
            slen = newpos
        self.len = slen
        self.pos = newpos

    def writelines(self, iterable):
        """Write a sequence of strings to the file. The sequence can be any
        iterable object producing strings, typically a list of strings. There
        is no return value.

        (The name is intended to match readlines(); writelines() does not add
        line separators.)
        """
        write = self.write
        for line in iterable:
            write(line)

    def flush(self):
        """Flush the internal buffer
        """
        _complain_ifclosed(self.closed)
        buf_len = len(self.buf)
        while self.pipeline and self._write_pos < buf_len:
            self.buf[self._write_pos] = self.pipeline.pop(0)
            self._write_pos += 1

        while self.pipeline and self._write_pos > buf_len:
            self.buf.append(0)
            buf_len += 1

        while self.pipeline:
            self.buf.append(self.pipeline.pop(0))
            self._write_pos += 1

    def getvalue(self):
        """
        Retrieve the entire contents of the "file" at any time before
        the StringIO object's close() method is called.

        The StringIO object can accept either Unicode or 8-bit strings,
        but mixing the two may take some care. If both are used, 8-bit
        strings that cannot be interpreted as 7-bit ASCII (that use the
        8th bit) will cause a UnicodeError to be raised when getvalue()
        is called.
        """
        _complain_ifclosed(self.closed)
        if self.pipeline:
            self.flush()
        return self.buf.decode(self.encoding)
try:
    from errno import EINVAL
except ImportError:
    EINVAL = 22

def _complain_ifclosed(closed):
    if closed:
        raise ValueError, "I/O operation on closed file"

class InMemImporter(object):
    def __init__(self, inmem_fs):
        self.inmem_fs = inmem_fs

    def find_module(self, fullname, path=None):
        if path:
            return None
        if fullname in self.inmem_fs.modules:
            return self
        return None

    def load_module(self, name):
        try:
            module = sys.modules[name] # already imported?
        except KeyError:
            module = imp.new_module(name)   # make new module
            sys.modules[name] = module      # make is visible

            # inject code at top of module to override normal import
            # and file open behaviour
            code = """
import sys
import os
setattr(os, 'chdir', inmem_fs.cd)
sys.meta_path = [inmem_fs.importer()]
open = inmem_fs.open_emulator
"""
            code += self.inmem_fs.get_source(name)         # append module code
            module.__dict__['inmem_fs'] = self.inmem_fs     # insert inmem_fs into new module

            # execute the code within the module's namespace
            try:
                exec code in module.__dict__
            except Exception as e:     # if code fails we need to
                if name in sys.modules:
                    del sys.modules[name]
                lines = [int(l) for l in re.findall('line (\d+)', str(e), re.DOTALL)]
                if lines:
                    extra = ('#'*10 + '\n').join([print_code_lines(code,
                                                                   line, 5) for line in lines])
                else:
                    extra = ''
                raise IOError("""In memory import of module "%s" failed with following:
%s'

%s""" % (name, e, extra))
        return module


class InMemOpener(BytesIO):
    def __init__(self, parent_fs, filename, mode, encoding='utf8'):
        assert isinstance(parent_fs, InMemFileSystem)
        self.parent = parent_fs
        self.open_mode = mode
        self.filename = filename

        if self.filename not in self.parent.files and mode[0] not in ['a', 'w']:
            raise IOError('[Errno 2] No such file or directory: %s' % filename)

        buf = self.parent.get_file(filename)
        super(InMemOpener, self).__init__(buf, mode=mode, encoding=encoding)

        if self.open_mode == 'r':
            self.seek(0)
        elif self.open_mode == 'w':
            self.seek(0)
            self.truncate()
        elif self.open_mode in ['w+', 'a', 'a+']:
            self.read()

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        self.close()


class InMemFileSystem(object):
    def __init__(self, parent=None, root=list(), cwdir=list()):
        if parent is None:
            if not root:
                root.append(self)
                cwdir.append(self)
                self.files = defaultdict(bytearray)
                self.parent = None
            else:
                self.files = cwdir[0].files
                self.parent = cwdir[0].parent
        else:
            self.files = defaultdict(bytearray)
            self.parent = parent

        self.cwdir = cwdir

    def cd(self, directory, dir_list=None):
        directory_list = dir_list or re.split('[/\\\\]+', directory)
        next_fs = None
        if directory_list[0] == '.':
            next_fs = self
        elif directory_list[0] == '..':
            if not self.parent:
                raise ('cannot cd up. no parent dir')
            next_fs = self.parent
        else:
            if directory_list[0] not in self.files:
                next_fs = self.files[directory_list[0]] = InMemFileSystem(parent=self)
            else:
                if not isinstance(self.files[directory_list[0]], InMemFileSystem):
                    raise ValueError('{0} is not a directory!'.format(directory_list[0]))
        if len(directory_list) > 1:
            return next_fs.cd(None, directory_list[1:])
        self.cwdir[0] = self
        return self

    def get_file(self, filename):
        if '/' in filename:
            idx = filename.find('/')
            folder = filename[:idx]
            filename = filename[(idx + 1):]
        elif '\\' in filename:
            idx = filename.find('\\')
            folder = filename[:idx]
            filename = filename[(idx + 1):]
        else:
            return self.files[filename]

        if folder in self.files:
            if not isinstance(self.files[folder], InMemFileSystem):
                raise ValueError('path is invalid\n\t{0}'.format(folder))
        else:
            self.files[filename] = InMemFileSystem(parent=self)
            try:
                f = self.files[filename].get_file(filename)
            except ValueError as e:
                e.message += '\n\t' + folder
                raise e
            return f

    def get_source(self, module_name):
        regexp = re.compile('^' + module_name + '(\.[ipybn]+)?')
        for fname, f in self.files.iteritems():
            r = regexp.findall(fname)
            if not r:
                continue
            if r[0]:
                with self.open_emulator(fname) as fp:
                    return self.file2source(fname, fp.read())
            raise NotImplementedError('importing packages in not implemented yet')
        raise ValueError('module not found')

    def write_file(self, filename, body):
        with self.open_emulator(filename, mode='w') as fp:
            fp.write(body)
            fp.truncate()

    def write_files(self, filename_body_pairs):
        for file_name, body in filename_body_pairs:
            self.write_file(file_name, body)

    def file2source(self, name, body):
        ext = name.split('.')[-1]
        if ext == 'ipynb':
            return self.ipynb2py(body)
        elif ext == 'py':
            if re.findall("coding: utf-8", body):
                body = re.sub('coding: utf-8', '', body)
            return body
        else:
            raise IOError('python source: %s has unknown extension. Must be .py or .ipynb' % name)

    def ipynb2py(self, nb):
        nb_dict = json.loads(nb)
        cells = nb_dict['worksheets'][0]['cells']
        python_source = list()
        for c in cells:
            if c['cell_type'] == 'code':
                python_source.append(''.join(c['input']))
        return '\n'.join(python_source)

    def open_emulator(self, filename, mode='r', **kwargs):
        return InMemOpener(self, filename, mode, **kwargs)

    def importer(self):
        return InMemImporter(self)


class InMemoryZip(object):
    def __init__(self):
        # Create the in-memory file-like object
        self.in_memory_zip = BytesIO()

    def append(self, filename_in_zip, file_contents):
        """Appends a file with name filename_in_zip and contents of
        file_contents to the in-memory zip."""
        # Get a handle to the in-memory zip in append mode
        zf = zipfile.ZipFile(self.in_memory_zip, "a", zipfile.ZIP_DEFLATED, False)

        # Write the file to the in-memory zip
        zf.writestr(filename_in_zip, file_contents)

        # Mark the files as having been created on Windows so that
        # Unix permissions are not inferred as 0000
        for zfile in zf.filelist:
            zfile.create_system = 0

        return self

    def read(self):
        """Returns a string with the contents of the in-memory zip."""
        self.in_memory_zip.seek(0)
        return self.in_memory_zip.read()
