from re import compile
import os
base_path='D:\\wilson\\'
file_meta={}

import os

def last_line(in_file, block_size=1024, ignore_ending_newline=False):
	suffix = ""
	in_file.seek(0, os.SEEK_END)
	in_file_length = in_file.tell()
	seek_offset = 0

	while(-seek_offset < in_file_length):
		# Read from end.
		seek_offset -= block_size
		if -seek_offset > in_file_length:
			# Limit if we ran out of file (can't seek backward from start).
			block_size -= -seek_offset - in_file_length
			if block_size == 0:
				break
			seek_offset = -in_file_length
		in_file.seek(seek_offset, os.SEEK_END)
		buf = in_file.read(block_size)

		# Search for line end.
		if ignore_ending_newline and seek_offset == -block_size and buf[-1] == '\n':
			buf = buf[:-1]
		pos = buf.rfind('\n')
		if pos != -1:
			# Found line end.
			return buf[pos+1:] + suffix

		suffix = buf + suffix

	# One-line file.
	return suffix


def get_file_meta():
	with open(base_path+'env.properties') as file:
		for line in file:
			kv = line.strip().split('=')
			file_meta[kv[1]] = kv[0]
	return file_meta


class mk_object():
	def __init__(self,line):
		self.line = line

	def get_schema(self):
		r_id = self.line[0]
		m_file_nm = file_meta[r_id]
		ll = []
		with open(base_path+m_file_nm) as f:
			for line in f:
				ll.append(line.strip().split(','))
		return ll


def mk_string(line):
	o = mk_object(line)
	schema = o.get_schema()
	fh = []
	for field in schema:
		field[0] = line[int(field[1]):int(field[2])]
		try:
			data = eval(field[3])(field[0])
			fh.append(field[0])
		except Exception as te:
			print 'Error error ' + str(te) + " " + line 
			return 'Error ' + line
	return '|'.join(fh)

if __name__ == "__main__":
	file = open(base_path+"Data.txt", 'rb')
	get_file_meta()
	fh = file.next()
	ff = mk_string(last_line(file))
	file.close()
	with open(base_path+"Data.txt", 'r') as file:
		drugs = 0
		prc = ''
		ll = []
		for line in file:
			if line != "":	
				record_identifier = line[0]
				if record_identifier == "0" :
					fh = mk_string(line)
				elif record_identifier == "4":
					prc = mk_string(line)
					drugs = int(prc.split('|')[6])
					if drugs != 0 :
						continue
					else:
						ll.append('|'.join([fh,prc,"|||||",ff]))
				elif record_identifier == "5":
					com = mk_string(line)
					if drugs != 0:
						ll.append('|'.join([fh, prc, com, ff]))
						drugs -= 1
					else:
						ll.append('|'.join([fh, prc, "|||||", ff]))
				elif record_identifier == "8":
					ff = mk_string(line)

		for i in ll:
			print(i)

# if __name__ == "__main__":
	# file_meta = get_file_meta()
	# fh = ""
	# ff = ""
	# with open(base_path+"Data.txt", 'r') as file:
		# for line in file:
			# if line != "" :
				# record_identifier = line[0]
				# if record_identifier == "0":
					# fh = mk_string(line)
				# elif record_identifier == "4":
					# print mk_string(line)
				# elif record_identifier == "5":
