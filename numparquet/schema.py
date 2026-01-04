import numpy as np

from .thrift import parquet_thrift


class NumparquetSchemaElement:
    """A schema element for a NumparquetSchema.

    Parameters
    ----------
    schema_elements : `list` [`parquet_thrift.SchemaElement`]
        The set of thrift serialized schema elements that describe a
        column, including the parent and all children elements.
    """

    def __init__(self, schema_elements):

        self._is_list = False
        self._list_length = -1

        if len(schema_elements) == 1:
            name_element = schema_elements[0]
            dtype_element = schema_elements[0]
        else:
            name_element = schema_elements[0]
            if (logicalType := name_element.logicalType) is None:
                if name_element.converted_type != parquet_thrift.ConvertedType.LIST:
                    raise RuntimeError("Multiple schema elements only supports LIST")
            elif logicalType.LIST is None:
                raise RuntimeError("Multiple schema elements only supports LIST")
            else:
                self._is_list = True

                if len(schema_elements) != 3:
                    raise RuntimeError("List schema elements improperly stored.")
                dtype_element = schema_elements[2]

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
            elif logicalType.TIMESTAMP is not None:
                if logicalType.TIMESTAMP.unit.MILLIS is not None:
                    dtype = np.datetime64(0, "ms").dtype
                elif logicalType.TIMESTAMP.unit.MICROS is not None:
                    dtype = np.datetime64(0, "us").dtype
                elif logicalType.TIMESTAMP.unit.NANOS is not None:
                    dtype = np.datetime64(0, "ns").dtype
            else:
                raise NotImplementedError(f"LogicalType {logicalType} not supported.")

        elif (converted_type := dtype_element.converted_type) is not None:
            # Note that this is basically untested since I don't know
            # how to make the parquet writer use the old format here.
            if converted_type == parquet_thrift.ConvertedType.UINT_64:
                dtype = np.uint64
            elif converted_type == parquet_thrift.ConvertedType.UINT_32:
                dtype = np.uint32
            elif converted_type == parquet_thrift.ConvertedType.UINT_16:
                dtype = np.uint16
            elif converted_type == parquet_thrift.ConvertedType.UINT_8:
                dtype = np.uint8
            elif converted_type == parquet_thrift.ConvertedType.UTF8:
                dtype = "U1"
            elif converted_type == parquet_thrift.ConvertedType.TIMESTAMP_MILLIS:
                dtype = np.datetime64(0, "ms").dtype
            elif converted_type == parquet_thrift.ConvertedType.TIMESTAMP_MICROS:
                dtype = np.datetime64(0, "us").dtype
            elif converted_type == parquet_thrift.ConvertedType.LIST:
                pass
            else:
                raise NotImplementedError(f"Unsupported converted type {converted_type}.")
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
    def dtype_name(self):
        if self._dtype == "S1":
            return "bytes"
        elif self._dtype == "U1":
            return "string"
        else:
            return np.dtype(self._dtype).name

    @property
    def is_byte_array(self):
        return self._is_byte_array

    @property
    def is_list(self):
        return self._is_list

    @property
    def list_length(self):
        return self._list_length

    @list_length.setter
    def list_length(self, value):
        self._list_length = value

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

    @property
    def description(self):
        desc = f"'{self.name}': {self.dtype_name}"
        if self.is_list:
            desc += ", list"
            if self._list_length < 0:
                desc += " (unknown length)"
            else:
                desc += f" ({self._list_length} elements)"
        if self.nullable:
            desc += ", nullable"

        return desc

    def __repr__(self):
        if self._is_list:
            list_str = "is_list=False"
        else:
            list_str = "is_list=True"
            if self._list_length < 0:
                list_str += " (unknown length)"
            else:
                list_str += f" ({self._list_length} elements)"

        parts = [
            f"name='{self.name}'",
            f"dtype={self.dtype_name}",
            list_str,
            f"nullable={self.nullable}",
        ]

        return "NumparquetSchemaElement(" + ", ".join(parts) + ")"


class NumparquetSchema:
    """A Numparquet schema.

    Parameters
    ----------
    file_metadata : `parquet_thrift.FileMetadata`
        The thrift serialized file metadata.
    """

    def __init__(self, file_metadata):
        self._parquet_version = file_metadata.version

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
    def parquet_version(self):
        return self._parquet_version

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

    def __repr__(self):
        lines = ["-- columns --"]
        for column, element in self._schema_dict.items():
            lines.append(element.description)
        if len(self._metadata) > 0:
            lines.append("-- metadata --")
            for key, value in self._metadata.items():
                line = f"'{key}': '{value}'"
                lines.append(line)

        return "\n".join(lines)
