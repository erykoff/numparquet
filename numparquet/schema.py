import numpy as np

from .thrift import parquet_thrift


class NumparquetSchemaElement:
    """Docstring."""

    def __init__(self, schema_elements):

        self._is_list = False
        self._list_length = 0

        if len(schema_elements) == 1:
            name_element = schema_elements[0]
            dtype_element = schema_elements[0]
        else:
            name_element = schema_elements[0]
            if (logicalType := name_element.logicalType) is None:
                raise RuntimeError("Cannot have multiple schema elements without a logicalType set.")
            if logicalType.LIST is not None:
                self._is_list = True

                if len(schema_elements) != 3:
                    raise RuntimeError("List schema elements improperly stored.")
                dtype_element = schema_elements[2]
            else:
                raise NotImplementedError("Only LIST types of multi-schema elements are supported.")

        self._name = name_element.name
        self._path_in_schema = [element.name for element in schema_elements]
        # self._required = (name_element.repetition_type == parquet_thrift.FieldRepetitionType.REQUIRED)
        self._is_byte_array = False

        native_dtype = None
        if dtype_element.type == parquet_thrift.Type.BOOLEAN:
            native_dtype = np.bool_
        elif dtype_element.type == parquet_thrift.Type.INT32:
            native_dtype = np.int32
        elif dtype_element.type == parquet_thrift.Type.INT64:
            native_dtype = np.int64
        elif dtype_element.type == parquet_thrift.Type.FLOAT:
            native_dtype = np.float32
        elif dtype_element.type == parquet_thrift.Type.DOUBLE:
            native_dtype = np.float64
        elif dtype_element.type == parquet_thrift.Type.BYTE_ARRAY:
            self._is_byte_array = True
            native_dtype = "S1"
        elif dtype_element.type == parquet_thrift.Type.FIXED_LEN_BYTE_ARRAY:
            native_dtype = f"S{dtype_element.type_length}"
        elif dtype_element.type is None:
            native_dtype = None
        else:
            raise RuntimeError(f"Unknown column dtype {dtype_element.type}")

        dtype = None
        if (logicalType := dtype_element.logicalType) is not None:  # noqa: F841
            if (intType := logicalType.INTEGER) is not None:
                if intType.bitWidth == 64:
                    if intType.isSigned:
                        dtype = np.int64
                    else:
                        dtype = np.uint64
                elif intType.bitWidth == 32:
                    if intType.isSigned:
                        dtype = np.int32
                    else:
                        dtype = np.uint32
                elif intType.bitWidth == 16:
                    if intType.isSigned:
                        dtype = np.int16
                    else:
                        dtype = np.uint16
                elif intType.bitWidth == 8:
                    if intType.isSigned:
                        dtype = np.int8
                    else:
                        dtype = np.uint8
                else:
                    raise ValueError("Illegal logicalType!")
            elif logicalType.STRING is not None:
                dtype = "U1"
            elif logicalType.FLOAT16 is not None:
                # Overwrite native_dtype in this case.
                native_dtype = np.float16
                dtype = np.float16
            else:
                raise NotImplementedError(f"LogicalType {logicalType} not supported.")
        elif (converted_type := dtype_element.converted_type) is not None:  # noqa: F841
            raise NotImplementedError("converted_type not implemented yet.")
        else:
            dtype = native_dtype

        if dtype in (np.float32, np.float64):
            null_value = np.nan
        elif dtype in (np.int8, np.int16, np.int32, np.int64):
            null_value = -1
        elif dtype in (np.bool_,):
            null_value = True
        elif dtype in ("S1", "U1"):
            null_value = ""
        else:
            # Unsigned integers in particular
            null_value = 0

        self._native_dtype = native_dtype
        self._dtype = dtype
        self._null_value = null_value

        self._max_repetition_level = 0
        for element in schema_elements:
            if element.repetition_type == parquet_thrift.FieldRepetitionType.REPEATED:
                self._max_repetition_level += 1
        self._repetition_level_bit_width = int(np.ceil(np.log2(self._max_repetition_level + 1)))

        self._max_definition_level = 0
        for element in schema_elements:
            if element.repetition_type != parquet_thrift.FieldRepetitionType.REQUIRED:
                self._max_definition_level += 1
        self._definition_level_bit_width = int(np.ceil(np.log2(self._max_definition_level + 1)))

    @property
    def name(self):
        return self._name

    @property
    def path_in_schema(self):
        return self._path_in_schema

    @property
    def nullable(self):
        return self._max_definition_level > 0

    @property
    def native_dtype(self):
        return self._native_dtype

    @property
    def dtype(self):
        return self._dtype

    @property
    def is_byte_array(self):
        return self._is_byte_array

    @property
    def is_list(self):
        return self._is_list

    @property
    def null_value(self):
        return self._null_value

    @property
    def max_definition_level(self):
        return self._max_definition_level

    @property
    def definition_level_bit_width(self):
        return self._definition_level_bit_width

    @property
    def max_repetition_level(self):
        return self._max_repetition_level

    @property
    def repetition_level_bit_width(self):
        return self._repetition_level_bit_width


class NumparquetSchema:
    """Docstring."""

    def __init__(self, file_metadata):
        if file_metadata.version < 2:
            raise NotImplementedError("Version 1 not supported yet.")

        self._num_rows = file_metadata.num_rows

        self._schema_dict = {}
        elements = []
        num_columns = -1
        for element in file_metadata.schema:
            if element.name == "schema":
                num_columns = element.num_children
            else:
                elements.append(element)
                if len(elements) == 1:
                    name = element.name
                if element.num_children is None:
                    self._schema_dict[name] = NumparquetSchemaElement(elements)
                    elements = []

        if len(self._schema_dict) != num_columns:
            raise RuntimeError("Number of schema elements is inconsistent.")

        # Count nulls.
        self._null_count = {col: 0 for col in self.columns}

        for row_group in file_metadata.row_groups:
            for row_group_column in row_group.columns:
                md = row_group_column.meta_data
                # Note: for LIST this is the first element;
                # for nested I don't know.
                name = self.get_element_from_path(md.path_in_schema).name
                self._null_count[name] += md.statistics.null_count

        # Load metadata.
        self._metadata = {}
        self._arrow_schema_encoded = None
        for kv in file_metadata.key_value_metadata:
            if kv.key == "ARROW:schema":
                self._arrow_schema_encoded = kv.value
            else:
                self._metadata[kv.key] = kv.value

    @property
    def columns(self):
        return list(self._schema_dict.keys())

    @property
    def num_rows(self):
        return self._num_rows

    @property
    def metadata(self):
        return self._metadata

    @property
    def arrow_schema_encoded(self):
        return self._arrow_schema_encoded

    def get_null_count(self, column):
        """Get total number of nulls in a column."""
        return self._null_count[column]

    def get_element_from_path(self, path_in_schema):
        for name in self.columns:
            if self._schema_dict[name].path_in_schema == path_in_schema:
                return self._schema_dict[name]
        raise KeyError(f"Path in schema {path_in_schema} not found.")

    def __getitem__(self, key):
        return self._schema_dict[key]

    # TODO: Make a nice __repr__.
