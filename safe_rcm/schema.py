import xmlschema


def open_schema(fs, root, name, *, glob="*.xsd"):
    """fsspec-compatible way to open remote schema files

    Parameters
    ----------
    fs : fsspec.filesystem
        pre-instantiated fsspec filesystem instance
    root : str
        URL of the root directory of the schema files
    name : str
        File name of the schema to open.
    glob : str, default: "*.xsd"
        The glob used to find other schema files

    Returns
    -------
    xmlschema.XMLSchema
        The opened schema object
    """
    filelikes = [fs.open(u) for u in fs.glob(f"{root}/{glob}")]
    sources = sorted(filelikes, key=lambda f: f.full_name.endswith(name), reverse=True)

    return xmlschema.XMLSchema(sources)
