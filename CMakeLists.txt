cmake_minimum_required(VERSION 3.10)

set(CFLAGS "-std=c++11 -Wall -g -D_GNU_SOURCE -pthread -lm -fno-pic -O2 -lprofiler -fsanitize=address")
set(SGX_COMMON_CFLAGS -DFREQ="2000")
set(SGX_COMMON_CFLAGS "${SGX_COMMON_CFLAGS} \
  -DAVX512F \
  -DTEST_QUICKSORT \
  -DVERBOSE \
  -DNDEBUG \
  -DREPORT_JOIN_STATS \
  -DREPORT_COLUMNSORT_STATS \
  -DREPORT_QSORT_STATS \
  -DPIN_TABLE \
  -DPAD_SCHEMA \
  -DALIGNMENT=64 \
  -DOBLI_XCHG \
  -DCOLUMNSORT_APPENDS \
  -DIO_LOCK \
  -DREPORT_3P_STATS \
  -DREPORT_APPEND_STATS \
  -DREPORT_SORT_STATS")
######## SGX SDK Settings (from Intel SGX SDK) ########

IF(NOT DEFINED SGX_SDK)
    MESSAGE(STATUS "SGX_SDK is not defined. using /opt/intel/sgxsdk")
    SET(SGX_SDK /home/peeyush/SGX/sgx-sdk/sgxsdk/)
ENDIF()
IF (NOT DEFINED ENV{SGX_MODE})
    MESSAGE(STATUS "SGX_MODE is not defined. using SIM")
    SET(ENV{SGX_MODE} "SIM")
ENDIF()
IF(NOT DEFINED ENV{SGX_ARCH})
    MESSAGE(STATUS "SGX_ARCH is not defined. using x64")
    SET(ENV{SGX_ARCH} "x64")
ENDIF()
IF(CMAKE_SIZEOF_VOID_P EQUAL 8)
    MESSAGE(STATUS "64 bits compiler detected")
ELSE()
    MESSAGE(STATUS "32 bits compiler detected")
    SET(ENV{SGX_ARCH} "x86")
    MESSAGE(STATUS "SGX_ARCH: x86")
ENDIF()
IF($ENV{SGX_ARCH} MATCHES "x86")
    SET(SGX_COMMON_CFLAGS "${SGX_COMMON_CFLAGS} -m32")
    SET(SGX_LIBRARY_PATH "${SGX_SDK}/lib")
    SET(SGX_ENCLAVE_SIGNER "${SGX_SDK}/bin/x86/sgx_sign")
    SET(SGX_EDGER8R "${SGX_SDK}/bin/x86/sgx_edger8r")
ELSE()
    SET(SGX_COMMON_CFLAGS "${SGX_COMMON_CFLAGS} -m64")
    SET(SGX_LIBRARY_PATH "${SGX_SDK}/lib64")
    SET(SGX_ENCLAVE_SIGNER "${SGX_SDK}/bin/x64/sgx_sign")
    SET(SGX_EDGER8R "${SGX_SDK}/bin/x64/sgx_edger8r")
ENDIF()
IF($ENV{SGX_DEBUG} MATCHES "1")
    IF($ENV{SGX_PRERELEASE} MATCHES "1")
        MESSAGE(FATAL_ERROR "Cannot set SGX_DEBUG and SGX_PRERELEASE at the same time!")
    ENDIF()
ENDIF()
IF($ENV{SGX_DEBUG} MATCHES "1")
    SET(SGX_COMMON_CFLAGS "${SGX_COMMON_CFLAGS} -O0 -g")
ELSEIF(WITH_DEBUG)
    SET(SGX_COMMON_CFLAGS "${SGX_COMMON_CFLAGS} -O0 -g")
ELSE()
    SET(SGX_COMMON_CFLAGS "${SGX_COMMON_CFLAGS} -O2")
ENDIF()
IF($ENV{SGX_MODE} MATCHES "HW")
    MESSAGE(STATUS "Configuring with SGX_MODE= ${SGX_MODE}")
    SET(Urts_Library_Name "sgx_urts")
    SET(Trts_Library_Name "sgx_trts")
    SET(Service_Library_Name "sgx_tservice")
    SET(App_Link_Flags "-lsgx_uae_service_sim")
    SET(Vrf_Cert_File "vrfcert.signed.so_HW")
ELSE()
    SET(Urts_Library_Name "sgx_urts_sim")
    SET(Trts_Library_Name "sgx_trts_sim")
    SET(Service_Library_Name "sgx_tservice_sim")
    SET(App_Link_Flags "-lsgx_uae_service_sim")
ENDIF()

SET(App_Name "sgx_db")
FILE(GLOB App_Cpp_Files "${CMAKE_SOURCE_DIR}/app/*.cpp" "${CMAKE_SOURCE_DIR}/app/Edger8rSyntax/*.cpp" "${CMAKE_SOURCE_DIR}/app/TrustedLibrary/*.cpp")
SET(App_Include_Paths "-Iinclude -Iapp -I${SGX_SDK}/include -I${CMAKE_SOURCE_DIR}/include")
SET(App_C_Flags "${SGX_COMMON_CFLAGS} -fPIC -Wno-attributes ${App_Include_Paths}")
# Three configuration modes - Debug, prerelease, release
# Debug - Macro DEBUG enabled.
# Prerelease - Macro NDEBUG and EDEBUG enabled.
# Release - Macro NDEBUG enabled.
IF($ENV{SGX_DEBUG} MATCHES "1")
    SET(App_C_Flags "${App_C_Flags} -DDEBUG -UNDEBUG -UEDEBUG")
ELSEIF(WITH_DEBUG)
    SET(App_C_Flags "${App_C_Flags} -DDEBUG -UNDEBUG -UEDEBUG")
ELSEIF($ENV{SGX_PRERELEASE} MATCHES "1")
    SET(App_C_Flags "${App_C_Flags} -DNDEBUG -DEDEBUG -UDEBUG")
ELSE()
    SET(App_C_Flags "${App_C_Flags} -DNDEBUG -UEDEBUG -UDEBUG")
ENDIF()

SET(App_Cpp_Flags "${App_C_Flags} -std=c++11")


SET(App_Link_Flags "${App_Link_Flags} ${SGX_COMMON_CFLAGS} -L${SGX_LIBRARY_PATH} -l${Urts_Library_Name} -lpthread")
SET(Crypto_Library_Name "sgx_tcrypto")

FILE(GLOB Enclave_Cpp_Files "${CMAKE_SOURCE_DIR}/enclave/*.cpp")

SET(Enclave_Include_Paths "-Iinclude -I${CMAKE_SOURCE_DIR}/enclave -I${SGX_SDK}/include -I${SGX_SDK}/include/tlibc -I$(SGX_SDK)/include/libcxx")

SET(Enclave_C_Flags "${SGX_COMMON_CFLAGS} -nostdinc -fvisibility=hidden -fpie -ffunction-sections -fdata-sections -fstack-protector-strong  ${Enclave_Include_Paths}")

SET(Enclave_Cpp_Flags "${Enclave_C_Flags} -std=c++11 -nostdinc++ -mavx512f")

SET(Enclave_Link_Flags "${SGX_COMMON_CFLAGS} -Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -L${SGX_LIBRARY_PATH} \
	-Wl,--whole-archive -l${Trts_Library_Name} -Wl,--no-whole-archive \
	-Wl,--start-group -lsgx_tstdc -lsgx_tcxx -l${Crypto_Library_Name} -l${Service_Library_Name} -Wl,--end-group \
	-Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined \
	-Wl,-pie,-eenclave_entry -Wl,--export-dynamic  \
	-Wl,--defsym,__ImageBase=0 -Wl,--gc-sections   \
	-Wl,--version-script=${CMAKE_SOURCE_DIR}/enclave/enclave.lds")

SET(Enclave_Name "enclave.so")
SET(Signed_Enclave_Name "enclave.signed.so")
SET(Signed_Vrfcert_Name "vrfcert.signed.so")
SET(Enclave_Config_File "${CMAKE_SOURCE_DIR}/enclave/config.xml")

IF($ENV{SGX_MODE} MATCHES "HW")
    IF(NOT $ENV{SGX_DEBUG} MATCHES "1")
        IF(NOT $ENV{SGX_PRERELEASE} MATCHES "1")
            SET(Build_Mode "HW_RELEASE")
        ENDIF()
    ENDIF()
ENDIF()


ADD_SUBDIRECTORY(enclave)
ADD_SUBDIRECTORY(app)
ADD_CUSTOM_TARGET(${Signed_Enclave_Name}
        ALL
        DEPENDS ${Enclave_Name}
        )
ADD_CUSTOM_COMMAND(TARGET "${Signed_Enclave_Name}"
        PRE_BUILD
        COMMAND "${SGX_ENCLAVE_SIGNER}" sign -key "${CMAKE_SOURCE_DIR}/enclave/enclave_private.pem" -enclave $<TARGET_FILE:${Enclave_Name}> -out "${Signed_Enclave_Name}" -config "${Enclave_Config_File}"
        COMMAND echo "'SIGN => ${Signed_Enclave_Name}'"
        )
INCLUDE_DIRECTORIES(${SGX_SDK}/include
        ${CMAKE_SOURCE_DIR}/enclave
        ${CMAKE_SOURCE_DIR}/app
        )
####