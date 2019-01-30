# sgx-db

Application is tested only on Linux

### Build instructions
* Check out `sgx` branch
* Download Intel SGX SDK from [here](https://01.org/intel-software-guard-extensions/downloads)
* Source sgxsdk environment
  ```~bash
  source /path/to/your/sgxsdk/environment
  ```
* To build in simulation debug mode
  `make SGX_MODE=SIM`
* To run
  `./db`
* To debug
  `sgx-gdb ./db`
