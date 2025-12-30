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


PyDoc_STRVAR(read_bitpacked_doc,
             "read_bitpacked()\n"
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
             "\n"
             "Returns\n"
             "-------\n"
             "\n"
             );

static PyObject *read_bitpacked(PyObject *dummy, PyObject *args) {
    PyObject *raw_bytes_obj = NULL;
    PyObject *raw_bytes_arr, *value_arr = NULL;

    // NpyIter *iter = NULL;

    int width;
    int count;
    // static char *kwlist[] = {"raw_bytes", "width", "count", NULL}

    if (!PyArg_ParseTuple(args, "Oii", &raw_bytes_obj, &width, &count))
        goto fail;

    raw_bytes_arr = PyArray_FROM_OTF(raw_bytes_obj, NPY_UINT8, NPY_ARRAY_IN_ARRAY | NPY_ARRAY_ENSUREARRAY);
    if (raw_bytes_arr == NULL) goto fail;
    uint8_t *raw_bytes_data = (uint8_t *)PyArray_DATA((PyArrayObject *)raw_bytes_arr);

    npy_intp dims[1];
    dims[0] = (npy_intp)count;

    value_arr = PyArray_SimpleNew(1, dims, NPY_INT32);
    if (value_arr == NULL) goto fail;
    int32_t *value_data = (int32_t *)PyArray_DATA((PyArrayObject *)value_arr);

    npy_intp raw_bytes_size = PyArray_SIZE(raw_bytes_arr);

    int current_byte = 0;
    int data = (int) raw_bytes_data[0];
    int mask = (1 << width) - 1;
    int bits_wnd_l = 8;
    int bits_wnd_r = 0;
    int total = (int) raw_bytes_size * 8;
    int index = 0;
    // fprintf(stdout, "%d\n", (int) mask);
    while (total >= width) {
        // Note zero-padding could produce extra zero values.
        // fprintf(stdout, "%d %d %d %d %d\n", current_byte, data, bits_wnd_l, bits_wnd_r, total);
        if (bits_wnd_r >= 8) {
            bits_wnd_r -= 8;
            bits_wnd_l -= 8;
            data >>= 8;
        } else if ((bits_wnd_l - bits_wnd_r) >= width) {
            value_data[index] = (int32_t) ((data >> bits_wnd_r) & mask);
            // fprintf(stdout, "%d: %d\n", index, value_data[index]);
            index++;
            total -= width;
            bits_wnd_r += width;
        } else if ((current_byte + 1) < raw_bytes_size) {
            current_byte++;
            // fprintf(stdout, "  %d %d %d\n", data, (int) raw_bytes_data[current_byte], bits_wnd_l);
            data |= ((int) (raw_bytes_data[current_byte]) << bits_wnd_l);
            // fprintf(stdout, "  -> %d\n", data);
            bits_wnd_l += 8;
        }
    }

    Py_DECREF(raw_bytes_arr);

    return PyArray_Return((PyArrayObject *)value_arr);

 fail:
    Py_XDECREF(raw_bytes_arr);
    Py_XDECREF(value_arr);
    // if (iter != NULL) {
    //     NpyIter_Deallocate(iter);
    // }

    return NULL;
}

static PyMethodDef numparquet_methods[] = {
    {"_read_bitpacked", (PyCFunction)(void (*)(void))read_bitpacked,
     METH_VARARGS, read_bitpacked_doc},
    {NULL, NULL, 0, NULL}};

static struct PyModuleDef numparquet_module = {PyModuleDef_HEAD_INIT, "_numparquet", NULL, -1,
                                               numparquet_methods};

PyMODINIT_FUNC PyInit__numparquet(void) {
    import_array();
    return PyModule_Create(&numparquet_module);
}
