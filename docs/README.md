# NoisePage Developer Docs

**Table of Contents**

- [New Students](#new-students)
- [Getting Started](#getting-started)

## New Students

Hi! Welcome to CMU DB.

1. Ask Andy to add you to the [noisepage-dev](https://mailman.srv.cs.cmu.edu/mailman/listinfo/noisepage-dev) mailing list. This will also subscribe you to the [db-seminar](https://mailman.srv.cs.cmu.edu/mailman/listinfo/db-seminar) mailing list for CMU-DB events.
2. Ask Andy to add you to the CMU-DB Slack channel. Make sure you join these channels:
   - `#general` -- Post all development-related questions here.
   - `#random` -- Random DB / CS / CMU stuff.
3. If you need access to testing or development machines, ask on Slack in `#dev-machines`. Check the pinned message in `#dev-machines` for more details.
4. (COVID19) ~~If you would like a seat in the ninth floor lab (GHC 9022), please email [Jessica Packer](http://csd.cs.cmu.edu/people/staff/jessica-packer) (CC Andy) to get a key. Important: Please make sure to **return the key before you leave CMU**.~~
5. (COVID19) Due to COVID19, we no longer meet on campus. Instead, we hang out on Zoom; just join the links in `#random`. The Zoom lab isn't just for doing work - feel free to cook, do other work, or live-stream your pets walking around.
6. Please follow the instructions in [Getting Started](#getting-started) below.

## Getting Started

### System Setup

1. **GitHub** We use GitHub for all our development.
   - **Account** [Sign up](https://github.com/join) for a GitHub account.
   - **Fork** Visit the [NoisePage repository](https://github.com/cmu-db/noisepage). Click the `Fork` button in the top right and fork the repository. You should be able to see the forked repository at `https://github.com/YOUR_GITHUB_USERNAME/noisepage`.
   - **SSH key** You should add a [SSH key](https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/adding-a-new-ssh-key-to-your-github-account) to your GitHub account.  
2. **OS** Make sure you are running [Ubuntu 20.04](https://releases.ubuntu.com/20.04/) or macOS 10.14+. If not, the recommended approach is to dual boot or to use a VM.
3. **IDE** We officially only support [CLion](https://www.jetbrains.com/clion/).
   - You can download CLion for free with the generous [Jetbrains educational license](https://www.jetbrains.com/community/education/#students).
   - More setup instructions are available [here](https://github.com/cmu-db/noisepage/tree/master/docs/tech_clion.md). This includes general CMake build flags.
4. **Packages** This is covered in the CLion setup, but as a reminder:
   - The default CLion cloned repository location is `~/CLionProjects/noisepage`.
   - Go to the folder: `cd ~/CLionProjects/noisepage/script/installation`
   - Install all the necessary packages: `sudo bash ./packages.sh`

### Further reading

You should learn a little about the following:

1. [CLion](https://github.com/cmu-db/noisepage/tree/master/docs/tech_clion.md)
2. [git](https://github.com/cmu-db/noisepage/tree/master/docs/tech_git.md)
3. [C++ and how we use it](https://github.com/cmu-db/noisepage/tree/master/docs/cpp_guidelines.md)

### Development 

1. Lorem ipsum doggo amet

