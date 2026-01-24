/*
 *  Copyright (C) 2025
 *  Author: Eli Rykoff
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#include <Python.h>

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION

#include <numpy/arrayobject.h>
#include <stdio.h>
#include <stdint.h>


PyDoc_STRVAR(decode_bitpacked_doc,
             "decode_bitpacked(raw_bytes, width, count, boolean=True)\n"
             "--\n\n"
             "Read a bitpacked array.\n"
             "\n"
             "Parameters\n"
             "----------\n"
             "raw_bytes : `np.ndarray`\n"
             "    Array (np.uint8) with raw bytes.\n"
             "width : `int`\n"
             "    Bit width.\n"
             "count : `int`\n"
             "    Number of values to unpack.\n"
             "boolean : `bool`, optional\n"
             "    Return boolean array if width is 1?\n"
             "\n"
             "Returns\n"
             "-------\n"
             "\n"
             );

static PyObject *decode_bitpacked(PyObject *dummy, PyObject *args, PyObject *kwargs) {
    PyObject *raw_bytes_obj = NULL;
    PyObject *raw_bytes_arr = NULL, *value_arr = NULL;

    int boolean = 0;

    int width;
    int count;
    static char *kwlist[] = {"raw_bytes", "width", "count", "boolean", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oii|p", kwlist, &raw_bytes_obj, &width, &count, &boolean))
        goto fail;

    raw_bytes_arr = PyArray_FROM_OTF(raw_bytes_obj, NPY_UINT8, NPY_ARRAY_IN_ARRAY | NPY_ARRAY_ENSUREARRAY);
    if (raw_bytes_arr == NULL) goto fail;
    uint8_t *raw_bytes_data = (uint8_t *)PyArray_DATA((PyArrayObject *)raw_bytes_arr);

    npy_intp dims[1];
    dims[0] = (npy_intp)count;

    void *value_data;
    int npy_type;
    if (boolean) {
        if (width != 1) {
            PyErr_SetString(PyExc_ValueError,
                            "boolean can only be True if width == 1.");
            goto fail;
        }
        npy_type = NPY_BOOL;
    } else {
        if (width <= 8) {
            npy_type = NPY_UINT8;
        } else if (width <= 16) {
            npy_type = NPY_UINT16;
        } else if (width <= 32) {
            npy_type = NPY_UINT32;
        } else if (width <= 64) {
            npy_type = NPY_UINT64;
        } else {
            PyErr_SetString(PyExc_ValueError,
                            "width must not be greater than 64.");
        }
    }
    value_arr = PyArray_SimpleNew(1, dims, npy_type);
    value_data = (void *)PyArray_DATA((PyArrayObject *)value_arr);

    npy_intp raw_bytes_size = PyArray_SIZE((PyArrayObject *)raw_bytes_arr);

    uint32_t current_byte = 0;
    uint64_t data = (uint64_t) raw_bytes_data[0];
    uint64_t mask = (1 << width) - 1;
    int64_t bits_wnd_l = 8;
    int64_t bits_wnd_r = 0;
    uint64_t total = (uint64_t) raw_bytes_size * 8;
    uint64_t index = 0;
    while (total >= width) {
        // Note zero-padding may produce extra zero values.
        if (bits_wnd_r >= 8) {
            bits_wnd_r -= 8;
            bits_wnd_l -= 8;
            data >>= 8;
        } else if ((bits_wnd_l - bits_wnd_r) >= width) {
            if (width <= 8) {
                ((uint8_t *)value_data)[index] = (uint8_t) ((data >> bits_wnd_r) & mask);
            } else if (width <= 16) {
                ((uint16_t *)value_data)[index] = (uint16_t) ((data >> bits_wnd_r) & mask);
            } else if (width <= 32) {
                ((uint32_t *)value_data)[index] = (uint32_t) ((data >> bits_wnd_r) & mask);
            } else {
                ((uint64_t *)value_data)[index] = (uint64_t) ((data >> bits_wnd_r) & mask);
            }
            index++;
            total -= width;
            bits_wnd_r += width;
        } else if ((current_byte + 1) < raw_bytes_size) {
            current_byte++;
            data |= ((uint64_t) (raw_bytes_data[current_byte]) << bits_wnd_l);
            bits_wnd_l += 8;
        }
    }

    Py_DECREF(raw_bytes_arr);

    return PyArray_Return((PyArrayObject *)value_arr);

 fail:
    Py_XDECREF(raw_bytes_arr);
    Py_XDECREF(value_arr);

    return NULL;
}


static size_t compute_storage_size_bytes(int width, int n_values) {
    size_t storage_size_bytes;

    storage_size_bytes = (size_t) ceil(width * n_values / 8);

    return storage_size_bytes;
}


static size_t compute_buffer_size_bytes(int width, int n_values) {
    size_t storage_size_bytes, buffer_size_bytes;

    storage_size_bytes = compute_storage_size_bytes(width, n_values);
    buffer_size_bytes = (size_t) (ceil(storage_size_bytes / 16) + 1) * 16;

    return buffer_size_bytes;
}

static int bitpack_values_internal(void *values, size_t n_values, size_t value_width, int width, uint8_t *output_buffer, size_t output_buffer_size) {
    int i, byte_offset, bit_offset;
    uint64_t v;

    uint64_t *buffered_values = NULL;

    buffered_values = (uint64_t *) calloc(1, sizeof(uint64_t));
    if (buffered_values == NULL) goto fail;

    bit_offset = 0;
    byte_offset = 0;

    if ((byte_offset + 8) > output_buffer_size) {
        return 0;
    }

    for (i=0; i<n_values; i++) {
        if (value_width == 1) {
            v = (uint64_t) ((uint8_t *) values)[i];
        } else if (value_width == 2) {
            v = (uint64_t) ((uint16_t *) values)[i];
        } else if (value_width == 32) {
            v = (uint64_t) ((uint32_t *) values)[i];
        } else {
            v = ((uint64_t *) values)[i];
        }

        buffered_values[0] |= (v << bit_offset);
        bit_offset += width;

        if (bit_offset >= 64) {
            memcpy(output_buffer + byte_offset, buffered_values, 8);

            buffered_values[0] = 0;
            byte_offset += 8;
            bit_offset -= 64;
            if ((width - bit_offset) != 64) {
                buffered_values[0] = (v >> (width - bit_offset));
            }
            if ((byte_offset + 8) > output_buffer_size) {
                return 0;
            }
        }
    }
    // Final flush
    memcpy(output_buffer + byte_offset, buffered_values, 8);

    free(buffered_values);

    return 1;

 fail:
    if (buffered_values != NULL) free(buffered_values);

    return 0;
}


PyDoc_STRVAR(encode_bitpacked_doc,
             "encode_bitpacked()\n"
             "--\n\n"
             "Bitpack values (up to 64 bits).\n"
             "\n"
             "Parameters\n"
             "----------\n"
             "values : `np.ndarray`\n"
             "    Array to pack.\n"
             "width : `int`\n"
             "    Bit-width for packing.\n"
             "\n"
             "Returns\n"
             "-------\n"
             "bit_packed_array : `np.ndarray`\n"
             "    Bit-packed array, of type np.uint8.\n"
             );


static PyObject *encode_bitpacked(PyObject *dummy, PyObject *args, PyObject *kwargs) {
    PyObject *values_obj = NULL;
    PyObject *values_arr = NULL, *output_arr = NULL;
    PyObject *slice = NULL, *retval = NULL;
    int width;
    npy_intp dims[1];
    npy_intp n_values;
    size_t output_buffer_size;
    uint8_t *output_buffer;
    void *values_buffer;
    size_t value_width;
    int value_type;
    int success;

    static char *kwlist[] = {"values", "width", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi", kwlist, &values_obj, &width))
        goto fail;

    values_arr = PyArray_FROM_OF(values_obj, NPY_ARRAY_ENSUREARRAY | NPY_ARRAY_C_CONTIGUOUS);
    if (values_arr == NULL) goto fail;

    n_values = PyArray_SIZE((PyArrayObject *)values_arr);
    output_buffer_size = compute_buffer_size_bytes(width, n_values);

    dims[0] = (npy_intp) output_buffer_size;
    output_arr = PyArray_SimpleNew(1, dims, NPY_UINT8);
    if (output_arr == NULL) goto fail;

    output_buffer = (uint8_t *) PyArray_DATA((PyArrayObject *)output_arr);
    values_buffer = (void *) PyArray_DATA((PyArrayObject *)values_arr);

    value_type = PyArray_TYPE((PyArrayObject *)values_arr);
    if ((value_type == NPY_BOOL) || (value_type == NPY_UINT8) || (value_type == NPY_INT8)) {
        value_width = 1;
    } else if ((value_type == NPY_UINT16) || (value_type == NPY_INT16)) {
        value_width = 2;
    } else if ((value_type == NPY_UINT32) || (value_type == NPY_INT32)) {
        value_width = 4;
    } else if ((value_type == NPY_UINT64) || (value_type == NPY_INT64)) {
        value_width = 8;
    } else {
        PyErr_SetString(PyExc_ValueError, "Can only pack integer or boolean types.");
        goto fail;
    }

    success = bitpack_values_internal(values_buffer, n_values, value_width, width, output_buffer, output_buffer_size);
    if (success == 0) {
        PyErr_SetString(PyExc_RuntimeError,
                        "bitpack_values failed.");
        goto fail;
    }

    slice = PySlice_New(PyLong_FromLong(0), PyLong_FromLong(compute_storage_size_bytes(width, n_values)), PyLong_FromLong(1));
    if (slice == NULL) goto fail;

    retval = PyObject_GetItem((PyObject *)output_arr, slice);
    if (retval == NULL) goto fail;

    Py_DECREF(slice);
    Py_DECREF(values_arr);
    Py_DECREF(output_arr);

    return PyArray_Return((PyArrayObject *)retval);

 fail:
    Py_XDECREF(values_arr);
    Py_XDECREF(output_arr);
    Py_XDECREF(slice);
    Py_XDECREF(retval);

    return NULL;
}


static PyMethodDef numparquet_methods[] = {
    {"_decode_bitpacked", (PyCFunction)(void (*)(void))decode_bitpacked,
     METH_VARARGS | METH_KEYWORDS, decode_bitpacked_doc},
    {"_encode_bitpacked", (PyCFunction)(void (*)(void))encode_bitpacked,
     METH_VARARGS | METH_KEYWORDS, encode_bitpacked_doc},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef numparquet_module = {PyModuleDef_HEAD_INIT, "_numparquet", NULL, -1,
                                               numparquet_methods};

PyMODINIT_FUNC PyInit__numparquet(void) {
    import_array();
    return PyModule_Create(&numparquet_module);
}
