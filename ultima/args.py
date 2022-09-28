class Args:
    """
    Canonized representation of task arguments.

    This class is used internally by the library, but can also be provided as a valid argument by users.
    It is specifically necessary when providing a tuple as the single argument for a task's function.

    Parameters
    ----------
    *args
        Positional arguments.
    **kwargs
        Keyword arguments.

    Attributes
    ----------
    raw
    """
    def __init__(self, *args, **kwargs):
        if '_created_from' in kwargs:
            self._created_from = kwargs.pop('_created_from')
        self.args = args
        self.kwargs = kwargs

    @property
    def raw(self):
        """Raw argument that was wrapped in this Args wrapper. Used internally."""
        return getattr(self, '_created_from', self)

    @classmethod
    def canonize(cls, item) -> 'Args':
        """
        Convert `item` into an `Args` object.

        The method provides convenient sugaring: If the given `item` is a tuple, it is automatically unpacked.
        In any case, the original `item` is saved and available for retrieval later on via the `raw` attribute.

        Parameters
        ----------
        item : Args, tuple or any other object
            - Args:      If the user has already provided an Args instance, returns the instance itself as is.
            - tuple:     In the special case `item` is a tuple, unpacks the tuple.
            - Otherwise: Used as a single input argument.

        Returns
        -------
        Args
            Canonized object representing the given `item` as arguments for a task.
        """
        if isinstance(item, cls):
            assert item.raw is item
            return item
        elif isinstance(item, tuple):
            return cls(*item, _created_from=item)
        else:
            return cls(item, _created_from=item)
