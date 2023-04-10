


# with open('D:/Download')
from os import listdir
from os.path import isfile, join
import glob

mypath='D:/Download'
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

# print(onlyfiles)

print(glob.glob(mypath + "/*"))



if __name__ == '__main__':
    pass
