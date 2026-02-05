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
#include <stdbool.h>


PyDoc_STRVAR(decode_bitpacked_doc,
             "decode_bitpacked(raw_bytes, bit_width, count, boolean=False)\n"
             "--\n\n"
             "Read a bitpacked array.\n"
             "\n"
             "Parameters\n"
             "----------\n"
             "raw_bytes : `np.ndarray`\n"
             "    Array (np.uint8) with raw bytes.\n"
             "bit_width : `int`\n"
             "    Bit width.\n"
             "count : `int`\n"
             "    Number of values to unpack.\n"
             "boolean : `bool`, optional\n"
             "    Return boolean array if bit_width is 1?\n"
             "\n"
             "Returns\n"
             "-------\n"
             "value_arr : `np.ndarray`\n"
             "    Array with unpacked values. Type depends on bit width;\n"
             "    will be boolean if ``boolean`` is True and bit_width is 1."
             );

static PyObject *decode_bitpacked(PyObject *dummy, PyObject *args, PyObject *kwargs) {
    PyObject *raw_bytes_obj = NULL;
    PyObject *raw_bytes_arr = NULL, *value_arr = NULL;

    int boolean = 0;

    int bit_width;
    int count;
    static char *kwlist[] = {"raw_bytes", "bit_width", "count", "boolean", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oii|p", kwlist, &raw_bytes_obj, &bit_width, &count, &boolean))
        goto fail;

    raw_bytes_arr = PyArray_FROM_OTF(raw_bytes_obj, NPY_UINT8, NPY_ARRAY_IN_ARRAY | NPY_ARRAY_ENSUREARRAY);
    if (raw_bytes_arr == NULL) goto fail;
    uint8_t *raw_bytes_data = (uint8_t *)PyArray_DATA((PyArrayObject *)raw_bytes_arr);

    npy_intp dims[1];
    dims[0] = (npy_intp)count;

    void *value_data;
    int npy_type;
    if (boolean) {
        if (bit_width != 1) {
            PyErr_SetString(PyExc_ValueError,
                            "boolean can only be True if bit_width == 1.");
            goto fail;
        }
        npy_type = NPY_BOOL;
    } else {
        if (bit_width <= 8) {
            npy_type = NPY_UINT8;
        } else if (bit_width <= 16) {
            npy_type = NPY_UINT16;
        } else if (bit_width <= 32) {
            npy_type = NPY_UINT32;
        } else if (bit_width <= 64) {
            npy_type = NPY_UINT64;
        } else {
            PyErr_SetString(PyExc_ValueError,
                            "bit_width must not be greater than 64.");
        }
    }
    value_arr = PyArray_SimpleNew(1, dims, npy_type);
    value_data = (void *)PyArray_DATA((PyArrayObject *)value_arr);

    npy_intp raw_bytes_size = PyArray_SIZE((PyArrayObject *)raw_bytes_arr);

    uint32_t current_byte = 0;
    uint64_t data = (uint64_t) raw_bytes_data[0];
    uint64_t mask = (1 << bit_width) - 1;
    int64_t bits_wnd_l = 8;
    int64_t bits_wnd_r = 0;
    uint64_t total = (uint64_t) raw_bytes_size * 8;
    uint64_t index = 0;
    while (total >= bit_width) {
        // Note zero-padding may produce extra zero values.
        if (bits_wnd_r >= 8) {
            bits_wnd_r -= 8;
            bits_wnd_l -= 8;
            data >>= 8;
        } else if ((bits_wnd_l - bits_wnd_r) >= bit_width) {
            if (bit_width <= 8) {
                ((uint8_t *)value_data)[index] = (uint8_t) ((data >> bits_wnd_r) & mask);
            } else if (bit_width <= 16) {
                ((uint16_t *)value_data)[index] = (uint16_t) ((data >> bits_wnd_r) & mask);
            } else if (bit_width <= 32) {
                ((uint32_t *)value_data)[index] = (uint32_t) ((data >> bits_wnd_r) & mask);
            } else {
                ((uint64_t *)value_data)[index] = (uint64_t) ((data >> bits_wnd_r) & mask);
            }
            index++;
            total -= bit_width;
            bits_wnd_r += bit_width;
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


typedef struct {
    uint8_t *buffer;
    size_t size;
    size_t index;
} numparquet_buffer;


static size_t compute_storage_size_bytes(int bit_width, int n_values) {
    size_t storage_size_bytes;

    storage_size_bytes = (size_t) ceil(bit_width * n_values / 8);

    return storage_size_bytes;
}


static size_t compute_buffer_size_bytes(int bit_width, int n_values) {
    size_t storage_size_bytes, buffer_size_bytes;

    storage_size_bytes = compute_storage_size_bytes(bit_width, n_values);
    buffer_size_bytes = (size_t) (ceil(storage_size_bytes / 16) + 1) * 16;

    return buffer_size_bytes;
}


static size_t get_value_width_internal(PyObject *values_arr) {
    int value_type;

    value_type = PyArray_TYPE((PyArrayObject *)values_arr);
    if ((value_type == NPY_BOOL) || (value_type == NPY_UINT8) || (value_type == NPY_INT8)) {
        return 1;
    } else if ((value_type == NPY_UINT16) || (value_type == NPY_INT16)) {
        return 2;
    } else if ((value_type == NPY_UINT32) || (value_type == NPY_INT32)) {
        return 4;
    } else if ((value_type == NPY_UINT64) || (value_type == NPY_INT64)) {
        return 8;
    } else {
        PyErr_SetString(PyExc_ValueError, "Can only pack integer or boolean types.");
        return 0;
    }
}


static int bitpack_values_internal(void *values, size_t n_values, size_t value_width, int bit_width, numparquet_buffer *buffer) {
    int i, byte_offset, bit_offset, num_bytes;
    uint64_t v;

    uint64_t *buffered_values = NULL;

    buffered_values = (uint64_t *) calloc(1, sizeof(uint64_t));
    if (buffered_values == NULL) goto fail;

    bit_offset = 0;
    byte_offset = 0;

    if ((buffer->index + 8) > buffer->size) {
        PyErr_SetString(PyExc_RuntimeError, "Bitpack buffer ran out of space.");
        return -1;
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
        bit_offset += bit_width;

        if (bit_offset >= 64) {
            memcpy(buffer->buffer + buffer->index + byte_offset, buffered_values, 8);

            buffered_values[0] = 0;
            byte_offset += 8;
            bit_offset -= 64;
            if ((bit_width - bit_offset) != 64) {
                buffered_values[0] = (v >> (bit_width - bit_offset));
            }
            if ((buffer->index + byte_offset + 8) > buffer->size) {
                PyErr_SetString(PyExc_RuntimeError, "Buffer ran out of space in bitpack_values_internal");
                goto fail;
            }
        }
    }
    // Final flush
    num_bytes = (bit_offset >> 3) + ((bit_offset & 7) != 0);
    memcpy(buffer->buffer + buffer->index + byte_offset, buffered_values, num_bytes);
    buffer->index += (byte_offset + num_bytes);

    free(buffered_values);

    return 0;

 fail:
    if (buffered_values != NULL) free(buffered_values);

    return -1;
}


PyDoc_STRVAR(encode_bitpacked_doc,
             "encode_bitpacked(values, bit_width)\n"
             "--\n\n"
             "Bitpack values (up to 64 bits).\n"
             "\n"
             "Parameters\n"
             "----------\n"
             "values : `np.ndarray`\n"
             "    Array to pack.\n"
             "bit_width : `int`\n"
             "    Bit-width for packing.\n"
             "\n"
             "Returns\n"
             "-------\n"
             "bit_packed_array : `np.ndarray`\n"
             "    Bit-packed array, of type np.uint8."
             );

/*
  Code adapted from arrow, arrow/cpp/src/arrow/util/rle_encoding_internal.h
  and arrow/cpp/src/arrow/util/bit_stream_utils_internal.h
 */
static PyObject *encode_bitpacked(PyObject *dummy, PyObject *args, PyObject *kwargs) {
    PyObject *values_obj = NULL;
    PyObject *values_arr = NULL, *output_arr = NULL;
    PyObject *slice = NULL, *retval = NULL;
    int bit_width;
    npy_intp dims[1];
    npy_intp n_values;
    void *values_buffer;
    size_t value_width;
    numparquet_buffer output_buffer;

    static char *kwlist[] = {"values", "bit_width", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi", kwlist, &values_obj, &bit_width))
        goto fail;

    values_arr = PyArray_FROM_OF(values_obj, NPY_ARRAY_ENSUREARRAY | NPY_ARRAY_C_CONTIGUOUS);
    if (values_arr == NULL) goto fail;

    n_values = PyArray_SIZE((PyArrayObject *)values_arr);
    output_buffer.size = compute_buffer_size_bytes(bit_width, n_values);

    dims[0] = (npy_intp) output_buffer.size;
    output_arr = PyArray_SimpleNew(1, dims, NPY_UINT8);
    if (output_arr == NULL) goto fail;

    output_buffer.buffer = (uint8_t *) PyArray_DATA((PyArrayObject *)output_arr);
    output_buffer.index = 0;
    values_buffer = (void *) PyArray_DATA((PyArrayObject *)values_arr);

    value_width = get_value_width_internal(values_arr);
    if (value_width == 0) goto fail;

    if (bitpack_values_internal(values_buffer, n_values, value_width, bit_width, &output_buffer) < 0) {
        goto fail;
    }

    slice = PySlice_New(PyLong_FromLong(0), PyLong_FromLong(output_buffer.index), PyLong_FromLong(1));
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


typedef struct {
    numparquet_buffer buffer;

    size_t value_width;

    int bit_width;
    int byte_width;
    int repeat_count;
    int literal_count;
    int num_buffered_values;
    void *literal_marker;
    uint64_t buffered_values[8];
    uint64_t current_value;

} numparquet_rle_packer;


static int encode_uleb128_internal(uint64_t value, numparquet_buffer *buffer) {
    uint8_t byte;

    do {
        byte = value & 0x7f; /* low-order 7 bits of value */
        value >>= 7;
        if (value != 0) /* more bytes to come */
            byte |= 0x80; /* set high-order bit of byte */
        buffer->buffer[buffer->index] = byte;
        buffer->index++;
        if (buffer->index == buffer->size) return -1;
    } while (value != 0);

    return 0;
}


static int store_repeated_run_internal(numparquet_rle_packer *packer) {
    uint64_t header;

    header = packer->repeat_count << 1;
    if (encode_uleb128_internal(header, &packer->buffer) < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Buffer ran out of space encoding uleb128");
        return -1;
    }

    if ((packer->buffer.index + packer->byte_width) > packer->buffer.size) {
        PyErr_SetString(PyExc_RuntimeError, "Buffer ran out of space for repeated run.");
        return -1;
    }

    memcpy(packer->buffer.buffer + packer->buffer.index, &packer->current_value, packer->byte_width);
    packer->buffer.index += packer->byte_width;

    packer->num_buffered_values = 0;
    packer->repeat_count = 0;

    return 0;
}


static int store_literal_run_internal(numparquet_rle_packer *packer, bool done) {
    int32_t num_groups;
    uint8_t indicator_value;

    if (packer->literal_marker == NULL) {
        if ((packer->buffer.index + 1) == packer->buffer.size) {
            PyErr_SetString(PyExc_RuntimeError, "Buffer ran out of space for literal run.");
            return -1;
        }
        packer->literal_marker = packer->buffer.buffer + packer->buffer.index;
        packer->buffer.index++;
    }

    if (packer->num_buffered_values > 0) {
        // Our buffer is always 64-bit.
        if (bitpack_values_internal(packer->buffered_values, packer->num_buffered_values, sizeof(uint64_t), packer->bit_width, &packer->buffer) < 0) return -1;
    }
    packer->num_buffered_values = 0;

    if (done) {
        num_groups = packer->literal_count / 8;
        indicator_value = (uint8_t) (num_groups << 1) | 1;
        memcpy(packer->literal_marker, &indicator_value, 1);
        packer->literal_count = 0;
        packer->literal_marker = NULL;
    }
    return 0;
}


static int store_buffered_values_internal(numparquet_rle_packer *packer, bool done) {
    int32_t num_groups;

    if (packer->repeat_count >= 8) {
        // Clear the buffer. They are part of a new repeated run and
        // we don't want to put them in the literal run.
        packer->num_buffered_values = 0;
        if (packer->literal_count != 0) {
            // There was a literal run, so store it.
            if (store_literal_run_internal(packer, true) < 0) {
                return -1;
            }
        }
        return 0;
    }

    packer->literal_count += packer->num_buffered_values;
    num_groups = packer->literal_count / 8;
    if ((num_groups + 1) >= (1 << 6)) {
        // We need to start a new literal run because the header
        // byte we reserved cannot store any more.
        if (store_literal_run_internal(packer, true) < 0) {
            return -1;
        }
    } else {
        if (store_literal_run_internal(packer, done) < 0) {
            return -1;
        }
    }

    return 0;
}


PyDoc_STRVAR(encode_rle_bitpacked_doc,
             "encode_rle_bitpacked(values, bit_width)\n"
             "--\n\n"
             "Bitpack values (up to 64 bits).\n"
             "\n"
             "Parameters\n"
             "----------\n"
             "values : `np.ndarray`\n"
             "    Array to pack.\n"
             "bit_width : `int`\n"
             "    Bit-width for packing.\n"
             "\n"
             "Returns\n"
             "-------\n"
             "rle_bit_packed_array : `np.ndarray`\n"
             "    Bit-packed array, of type np.uint8."
             );

/*
  Code adapted from arrow, arrow/cpp/src/arrow/util/rle_encoding_internal.h
  and arrow/cpp/src/arrow/util/bit_stream_utils_internal.h
 */
static PyObject *encode_rle_bitpacked(PyObject *dummy, PyObject *args, PyObject *kwargs) {
    PyObject *values_obj = NULL;
    PyObject *values_arr = NULL, *output_arr = NULL;
    PyObject *slice = NULL, *retval = NULL;
    int bit_width;
    npy_intp dims[1];
    npy_intp n_values;

    numparquet_rle_packer packer;

    uint64_t v;

    void *values_buffer;
    int i;

    static char *kwlist[] = {"values", "bit_width", NULL};

    packer.repeat_count = 0;
    packer.literal_count = 0;
    packer.num_buffered_values = 0;
    for (int i=0; i<8; i++) packer.buffered_values[i] = 0;
    packer.buffer.buffer = NULL;
    packer.literal_marker = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi", kwlist, &values_obj, &bit_width))
        goto fail;

    values_arr = PyArray_FROM_OF(values_obj, NPY_ARRAY_ENSUREARRAY | NPY_ARRAY_C_CONTIGUOUS);
    if (values_arr == NULL) goto fail;

    n_values = PyArray_SIZE((PyArrayObject *)values_arr);

    // The output buffer we set to be pessimistically the bit-packed size, plus
    // some extra which we will trim off at the end.
    dims[0] = (npy_intp) (bit_width * n_values + 100);
    // output_arr = PyArray_SimpleNew(1, dims, NPY_UINT8);
    output_arr = PyArray_ZEROS(1, dims, NPY_UINT8, false);
    if (output_arr == NULL) goto fail;

    packer.buffer.buffer = (uint8_t *) PyArray_DATA((PyArrayObject *)output_arr);
    packer.buffer.index = 0;
    packer.buffer.size = dims[0];
    packer.bit_width = bit_width;
    packer.byte_width = (packer.bit_width >> 3) + ((packer.bit_width & 7) != 0);
    packer.value_width = get_value_width_internal(values_arr);
    if (packer.value_width == 0) goto fail;

    values_buffer = (void *) PyArray_DATA((PyArrayObject *)values_arr);

    for (i=0; i<n_values; i++) {
        if (packer.value_width == 1) {
            v = (uint64_t) ((uint8_t *) values_buffer)[i];
        } else if (packer.value_width == 2) {
            v = (uint64_t) ((uint16_t *) values_buffer)[i];
        } else if (packer.value_width == 32) {
            v = (uint64_t) ((uint32_t *) values_buffer)[i];
        } else {
            v = ((uint64_t *) values_buffer)[i];
        }
        if (v == packer.current_value) {
            packer.repeat_count++;
            if (packer.repeat_count > 8) {
                continue;
            }
        } else {
            // The value has changed.
            // First check if this is the end of a long run.
            if (packer.repeat_count >= 8) {
                // End of a long run; store and reset.
                if (store_repeated_run_internal(&packer) != 1) {
                    PyErr_SetString(PyExc_RuntimeError,
                            "failed to serialize.");
                    goto fail;
                }
            }
            // Possibly the start of a new run!
            packer.repeat_count = 1;
            packer.current_value = v;
        }
        packer.buffered_values[packer.num_buffered_values++] = v;
        if (packer.num_buffered_values == 8) {
            if (store_buffered_values_internal(&packer, false) < 0) {
                goto fail;
            }
        }
    }
    if ((packer.literal_count > 0) || (packer.repeat_count > 0) || (packer.num_buffered_values > 0)) {
        // We have to close out existing buffered data.
        bool all_repeat = ((packer.literal_count == 0) & ((packer.repeat_count == packer.num_buffered_values) | (packer.num_buffered_values == 0)));
        if ((packer.repeat_count > 0) & (all_repeat)) {
            if (store_repeated_run_internal(&packer) < 0) {
                goto fail;
            }
        } else {
            // Pad with 0s
            for (; (packer.num_buffered_values != 0) && (packer.num_buffered_values < 8); packer.num_buffered_values++) {
                packer.buffered_values[packer.num_buffered_values] = 0;
            }
            packer.literal_count += packer.num_buffered_values;
            if (store_literal_run_internal(&packer, true) < 0) {
                goto fail;
            }
            packer.repeat_count = 0;
        }
    }

    slice = PySlice_New(PyLong_FromLong(0), PyLong_FromLong(packer.buffer.index), PyLong_FromLong(1));
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
    {"_encode_rle_bitpacked", (PyCFunction)(void (*)(void))encode_rle_bitpacked,
     METH_VARARGS | METH_KEYWORDS, encode_rle_bitpacked_doc},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef numparquet_module = {PyModuleDef_HEAD_INIT, "_numparquet", NULL, -1,
                                               numparquet_methods};

PyMODINIT_FUNC PyInit__numparquet(void) {
    import_array();
    return PyModule_Create(&numparquet_module);
}
