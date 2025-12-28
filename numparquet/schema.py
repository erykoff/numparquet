import numpy as np

from .thrift import parquet_thrift


class NumparquetSchemaElement:
    """Docstring."""

    def __init__(self, schema_element):
        self._name = schema_element.name
        # self._visible = (self._name != "schema")
        self._required = (schema_element.repetition_type == parquet_thrift.FieldRepetitionType.REQUIRED)

        native_dtype = None
        if schema_element.type == parquet_thrift.Type.BOOLEAN:
            native_dtype = np.bool_
        elif schema_element.type == parquet_thrift.Type.INT32:
            native_dtype = np.int32
        elif schema_element.type == parquet_thrift.Type.INT64:
            native_dtype = np.int64
        elif schema_element.type == parquet_thrift.Type.FLOAT:
            native_dtype = np.float32
        elif schema_element.type == parquet_thrift.Type.DOUBLE:
            native_dtype = np.float64
        elif schema_element.type is None:
            native_dtype = None
        else:
            # Note that type_length is FIXED_LEN_BYTE_ARRAY
            # This is the string maybe?  Will need to work on that.
            raise NotImplementedError("Sorry; byte dtypes not supported yet")

        dtype = None
        if (logicalType := schema_element.logicalType) is not None:  # noqa: F841
            raise NotImplementedError("Sorry; logical type not implemented yet.")
        elif (converted_type := schema_element.converted_type) is not None:  # noqa: F841
            raise NotImplementedError("Sorry; converted_type not implemented yet.")
        else:
            dtype = native_dtype

        if dtype in (np.float32, np.float64):
            null_value = np.nan
        elif dtype in (np.int16, np.int32):
            null_value = -1
        elif dtype in (np.bool_,):
            null_value = True
        else:
            null_value = 0

        self._native_dtype = dtype
        self._dtype = dtype
        self._null_value = null_value

    @property
    def name(self):
        return self._name

    @property
    def required(self):
        return self._required

    @property
    def native_dtype(self):
        return self._native_dtype

    @property
    def dtype(self):
        return self._dtype

    @property
    def null_value(self):
        return self._null_value


class NumparquetSchema:
    """Docstring."""

    def __init__(self, file_metadata):
        if file_metadata.version < 2:
            raise NotImplementedError("Version 1 not supported yet.")

        # What we want to do is (a) save the schema element separately?
        # and then go through and store a mapping from name to element.
        # and then we also need ... number of rows, etc.
        # fmd.key_value_metadata
        # fmd.num_rows
        # fmd.row_groups NO

        self._num_rows = file_metadata.num_rows

        self._schema_dict = {
            elt.name: NumparquetSchemaElement(elt) for elt in file_metadata.schema if elt.type is not None
        }

        # Count nulls.
        self._null_count = {col: 0 for col in self.columns}

        for row_group in file_metadata.row_groups:
            for row_group_column in row_group.columns:
                md = row_group_column.meta_data
                name = md.path_in_schema[-1]
                self._null_count[name] += md.statistics.null_count

        # key-value later.

    @property
    def columns(self):
        return list(self._schema_dict.keys())

    @property
    def num_rows(self):
        return self._num_rows

    def get_null_count(self, column):
        """Get total number of nulls in a column."""
        return self._null_count[column]

    def max_definition_level(self, path):
        """Get the max definition level for a given path.

        Parameters
        ----------
        path : `list` [`str`]

        Returns
        -------
        max_definition_level : `int`
        """
        max_level = 0
        for part in path:
            if not self._schema_dict[part].required:
                max_level += 1

        return max_level

    def __getitem__(self, key):
        return self._schema_dict[key]

    # TODO: Make a nice __repr__.
