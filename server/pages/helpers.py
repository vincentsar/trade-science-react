###################### GENERAL LIST FUNCTIONS ######################
def list_chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.

    Example usage:
    symbol_chunks = list(list_chunks(symbols, 20))
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
