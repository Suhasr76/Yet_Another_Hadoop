import os
from sys import platform
from prompt_toolkit import prompt
import subprocess
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
import shutil
from namenode import NameNode
import ModuleFiles


t1 = NameNode(daemon = True)
t1.start()

def help(args):

    if args == []:
        print("1. ls\t\t\t\t\t\t-List all the file and folders.\n\t-l\t\t\t\t\t-List all the file and folders in a detailed manner.\n\t-al\t\t\t\t\t-List the hidden files along wiht remaining files.")
        print(
            "2. cd [folder path]\t\t\t\t-To go the specified directory.\n   cd..\t\t\t\t\t\t-To go back to root directory.")
        print("3. cat [filename]\t\t\t\t-Display the contents of the file.")
        print("4. put [options] [source] [destination]\t\t-options(-r) - to move folder into another folder.\n\t\t\t\t\t\t-source - one or many files to be put.\n\t\t\t\t\t\t-destination - location of the file or folder.")
        print("5. rm [filename]\t\t\t\t-Remove the files in the given path.")
        print("6. rmdir [filename]\t\t\t\t-Remove the specified directory.")
        print(
            "7. mkdir [filename]\t\t\t\t-Create directory in the specified path.")
        print("8 . quit/exit\t\t\t\t\t-To stop the execution of the filesystem.\n")
        print(
            f"To run a Hadoop Job-\nyah{' '*1}--input [str, absolute path to input file inside fs_path]\n{' '*4}--output [str, absolute path to directory to store output and job status]\n{' '*4}--config [str, absolute path to dfs_setup_config to load DFS configuration]\n{' '*4}--mapper [str, absolute path to mapper.py]\n{' '*4}--reducer [str, absolute path to reducer.py]\n")
    else:
        print(
            f"help {' '.join(args)}: command not found\nEnter 'help' to know more..")


def cat(args):

    global t1
    if args == []:
        print("cat: no arguments specified")
        return
    for arg in args:
        # temp = subprocess.Popen(['cat', arg], stdout=subprocess.PIPE)
        # try:
        #     output = str(temp.communicate())
        #     output = output.split('\'')
        #     output = output[1].replace('\\r', '').split('\\n')
        #     for i in output:
        #         print(i)
        t1.cat(arg)
        # except Exception as e:
        #     print(e)


""" def put(args):

    cp = 'cp '
    if str(args[0])[0] == '-':
        cp = cp+args[0]+' '
    for i in args[:-1]:
        if i != '-r':
            command = cp+str(i) + ' ' + str(args[-1])
            try:
                os.system(command)

            except Exception as e:
                l = e.split('\n')
                print(l[0])
    print('\n') """


# def put(args):

#     # global t1
#     # if len(args) == 0:
#     #     print("put: no arguments specified")
#     #     return

#     # for loc in args:
#     #     try:
#     #         nnwrite = .put(loc)
#     #         t1.put(nnwrite)
#     #     except Exception as e:
#     #         print(e)


def ls(args):

    temp = 0
    if args != []:
        temp = subprocess.Popen(['ls', args[0]], stdout=subprocess.PIPE)
    else:
        temp = subprocess.Popen('ls', stdout=subprocess.PIPE)
    output = str(temp.communicate())
    output = output.split('\'')
    output = output[1].split('\\n')
    for line in output:
        if os.path.isdir(line):
            line = "./"+line
        print(line)


def rm(args):
    if args == []:
        print("rm: specify name(s) of file(s) to remove")
        return
    for i in args:
        if os.path.isfile(i):
            os.remove(i)
        elif os.path.isdir(i):
            print(f"rm: cannot remove '{i}': Is a directory")
        else:
            print(f"rm: cannot remove '{i}': No such file or directory")


def rmdir(args):
    if args == []:
        print("rmdir: specify name(s) of folder(s) to remove")
        return
    for i in args:
        if os.path.isdir(i):
            os.rmdir(i)
        elif os.path.isfile(i):
            print(f"rmdir: failed to remove '{i}': Not a directory")
        else:
            print(f"rmdir: failed to remove '{i}': No such file or directory")


def mkdir(args):
    if args == []:
        print("mkdir: specify name(s) of folder(s) to create")
        return
    for i in args:
        try:
            os.mkdir(i)
        except:
            print(f"mkdir: cannot create directory '{i}': File exists")


def quit(args):
    if args == []:
        print('\nGoodbye!!')
        return -1
    else:
        print(
            f"'quit {' '.join(args)}' not found!!\n Try 'quit' to quit the fs")


def exit(args):
    if args == []:
        print('\nGoodbye!!')
        return -1
    else:
        print(
            f"'exit {' '.join(args)}' not found!!\n Try 'exit' to quit the fs")


def cd(args):
    if len(args) == 1:
        try:
            os.chdir(args[0])
        except Exception as e:
            print(e)


def python(args):
    if len(args) != 0:
        temp = 0
        try:
            if platform == "linux" or platform == "linux2":
                temp = subprocess.Popen(
                    ['python3', args[0]], stdout=subprocess.PIPE)
            elif platform == 'win32':
                temp = subprocess.Popen(
                    ['python', args[0]], stdout=subprocess.PIPE)
        except Exception as e:
            print(e)
        output = temp.communicate()[0]
        output = str(output).split('\\n')
        count = 0
        for i in output[:-1]:
            if count == 0:
                i = i[2:].replace('\\r', '')
            else:
                i = i.replace('\\r', '')
            count += 1
            print(i)



comm = {
    'help': help,
    'cd': cd,
    'cat': cat,
    'ls': ls,
    'rm': rm,
    'rmdir': rmdir,
    'mkdir': mkdir,
    'quit': quit,
    'exit': exit,
    'python': python,
    'python3': python,
}


def err(fun, args):
    try:
        val = fun(args)
    except Exception as e:
        return e
    else:
        return val

#config = #reads from config.json
#dfsbdfb = #reads from dfs_setup_config(config) #open(config("dfs_setup_config"))

absroot = os.getcwd()
if os.path.isdir(f'{absroot}/logs') == False:
    os.mkdir(f'{absroot}/logs')

root = os.getcwd()
if str('\\') in root:
    root = root.split('\\')[-1]
elif '/' in root:
    root = root.split('/')[-1]



while 1:
    dir = os.getcwd()
    if '\\' in dir:
        dir = dir.split('\\')[-1]
    elif '/' in dir:
        dir = dir.split('/')[-1]
    print(t1.is_alive())
    user_input = prompt(dir+'>', auto_suggest=AutoSuggestFromHistory(),
                        history=FileHistory(f'{absroot}/logs/history.txt'))
    try:
        if user_input == 'cd..' or user_input == 'cd ..':
            if root == dir:
                print(f'cd: Can\'t go backwards beyond the root')
            else:
                os.chdir('..')
        elif user_input == '': pass
        else:
            userInputList = user_input.split(' ')
            command = comm[userInputList[0]]
            val = err(command, userInputList[1:])
            if val == -1:
                break
    except Exception as e:
        print(f"{e} : Unknown Command")
