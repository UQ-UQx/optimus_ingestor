from fabric.api import *
from fabric.contrib.console import confirm

verbose = True

import config

env.github_url = config.FAB_GITHUB_URL
env.remote_code_dir = config.FAB_REMOTE_PATH
env.hosts = config.FAB_HOSTS

def prepare():
    func_gitadd()
    func_gitpush()

def deploy():
    with hide('warnings'), settings(warn_only=True): #'output', 'running',
        if run("test -d %s" % env.remote_code_dir).failed:
            print "Warning: Working directory does not exist"
            sudo("mkdir -p "+env.remote_code_dir, "Creating Directory", True)
            sudo("git clone "+env.github_url+" %s" % env.remote_code_dir, "Cloning Github", True)
        else:
            remote_vc("%s/injestor.py stop" % env.remote_code_dir, "Stopping Injestor")
    with cd(env.remote_code_dir):
        remote_vc("git stash", "Resetting changes to remote", True)
        remote_vc("git reset --hard", "Resetting changes to remote", True)
        remote_vc("git pull", "Pulling from git", True)
        remote_vc("%s/injestor.py start" % env.remote_code_dir, "Starting Injestor")


def create():
    with hide('output', 'running', 'warnings'), settings(warn_only=True):
        if run("test -d %s" % env.remote_code_dir).failed:
            print "Source Directory does not exist: "+env.remote_code_dir

#Internal


def func_gitadd():
    git_message = prompt("[Local] Enter Git Commit Message: ", "Hotfix")
    if git_message == "":
        git_message = "Anonymous Hotfix"
    local_ve("git add . && git commit -a -m \"" + git_message + "\"", "Git adding", True)


def func_gitpush():
    local_ve("git push", "Pushing to github")


#Helpers

def local_ve(cmd, message, ignoreerror=False):
    if verbose:
        print "[Local] Command: " + message
    with hide('running'), settings(warn_only=True):
        result = local(cmd, capture=True)
        if not ignoreerror and result.failed and not confirm("+ Error: " + message + " failed. Continue anyway?"):
            abort("Aborting at user request.")

def remote_vc(cmd, message, showout=False):
    if verbose:
        print "["+env.host_string+"] Command: " + message
    with hide('running', 'warnings', 'output'), settings(warn_only=True):
        if showout is True:
            with show('output'):
                result = sudo(cmd)
                if result.failed and not confirm("+ Error: " + message + " failed. Continue anyway?"):
                    abort("Aborting at user request.")
        else:
            result = sudo(cmd)
            if result.failed and not confirm("+ Error: " + message + " failed. Continue anyway?"):
                abort("Aborting at user request.")
