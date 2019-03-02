set_source_files_properties(enclave_t.c PROPERTIES LANGUAGE CXX )

SET(CMAKE_C_FLAGS "${Enclave_C_Flags}")
SET(CMAKE_CXX_FLAGS "${Enclave_Cpp_Flags}")
INCLUDE_DIRECTORIES(
        ${SGX_SDK}/include
        ${CMAKE_SOURCE_DIR}/enclave
        ${CMAKE_SOURCE_DIR}/include
        ${SGX_SDK}/include/tlibc
        ${SGX_SDK}/include/libcxx
)
# EDGER Generated Files
SET(Enclave_T_Files enclave_t.c enclave_t.h)
SET_SOURCE_FILES_PROPERTIES(${Enclave_T_Files} PROPERTIES GENERATED TRUE)
# Add all the enclave functions including ecalls/ocalls and syscall wrappers
FILE(GLOB Enclave_Cpp_Files "*.cpp" "TrustedLibrary/*.cpp" "include/*.cpp")
FILE(GLOB Enclave_C_Files "*.c")
FILE(GLOB Enclave_H_Files "*.h" "*.hpp" "include/*.h" "include/*.hpp")
# Remove the EDGER generated files from the lists
LIST(REMOVE_ITEM Enclave_C_Files "${CMAKE_CURRENT_SOURCE_DIR}/enclave_t.c")
LIST(REMOVE_ITEM Enclave_H_Files "${CMAKE_CURRENT_SOURCE_DIR}/enclave_t.h")
# Generate the trusted files
ADD_CUSTOM_COMMAND(OUTPUT ${Enclave_T_Files}
        COMMAND "${SGX_EDGER8R}" --trusted enclave.edl --search-path "${CMAKE_SOURCE_DIR}/enclave" --search-path "${SGX_SDK}/include"
        COMMAND cp ${Enclave_T_Files} "${CMAKE_SOURCE_DIR}/enclave"
        COMMAND "echo" 'GEN => ${CMAKE_SOURCE_DIR}/enclave/enclave_t.c'
        )

ADD_CUSTOM_TARGET(Gen_Enclave_T ALL DEPENDS ${Enclave_T_Files})
# Create enclave libraries
ADD_LIBRARY(Enclave_lib ${Enclave_Cpp_Files} ${Enclave_T_Files} ${Enclave_H_Files})
ADD_DEPENDENCIES(Enclave_lib Gen_Enclave_T)
TARGET_LINK_LIBRARIES(Enclave_lib ${Enclave_Link_Flags})
# create and link the enclave library
ADD_LIBRARY(${Enclave_Name} MODULE ${Enclave_T_Files} ${Enclave_C_Files} ${Enclave_Cpp_Files} ${Enclave_H_Files})
TARGET_LINK_LIBRARIES(${Enclave_Name} ${Enclave_Link_Flags} )