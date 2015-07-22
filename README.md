#Monaco
**Monaco** is a cluster-aware system providing redis as a service. Using Raft for distributed leader state

#### Screenshot
![Monaco Screenshot](https://github.com/hulu/monaco/raw/master/MonacoSS.png)


## Installation
On Ubuntu precise:
```
    $ sudo apt-add-repository -ppa:hulu-tech/ppa
    $ sudo apt-get update
    $ sudo apt-get install monaco
```
On other Debian builds
```
    $ git clone https://github.com/hulu/monaco
    $ cd monaco
    $ sudo dpkg -i Monaco_1.1.deb
```
## Download
* I recommend you install via package (see above)
* [Latest Release](https://github.com/hulu/monaco/archive/master.zip)
* Installing from source:
```
    $ wget https://github.com/hulu/monaco/archive/master.zip
    $ unzip master.zip && cd master
    $ ./ubuntupkg.sh
    $ sudo dpkg -i Monaco_1.1.deb
```

## Contributors

### Contributors on GitHub
* [Contributors](https://github.com/hulu/monaco/graphs/contributors)

## License 
* see [LICENSE](https://github.com/hulu/monaco/blob/master/LICENSE.md) file

## Version 
* Version 1.0-1
  * Initial Release!

## Usage
* there is minimal set up required to get started with Monaco
* see [INSTRUCTIONS](https://github.com/hulu/monaco/blob/master/INSTRUCTIONS.md) file

## Contact
#### Keith Ainsworth/Hulu
* e-mail: <keith@hulu.com>
* [Hulu](http://www.hulu.com)
