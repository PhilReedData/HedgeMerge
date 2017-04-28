#!/usr/bin/python    
# Step 3.1
# Find duplicate funds in MergedCharacteristics

# We are populating the MergedFundID field

# MERGEDCHARACTERISTICS EXAMPLE
# Source, SourceFundID, StdCompanyName, Currency, [CompCurr], MergedFundID
# T,      1,            Apple,          USD,      1,          ?
# T,      2,            Apple,          USD,      1,          ?
# T,      3,            Apple,          GBP,      2,            ?
# T,      4,            Banana,         USD,      3,              ?
# T,      5,            Banana,         USD,      3,              ?
# T,      6,            ,               EUR,      ,
# E,      10,           Apple,          USD,      1,          ?
# E,      11,           Apple,          GBP,      2,            ?
# E,      12,           Carrot,         USD,      4,                 ?
# E,      13,           Carrot,         USD,      4,                 ?
# E,      14,           Date,           EUR,      5,                   ?
# E,      15,           Apple Pie,      USD,      6,                     ?

# [CompCurr] is not saved in the table, 
# it just illustrates which sets of funds are checked in pairs for return correln.

# THOUGHT: sorting by StdCompanyName,Currency will make things easier...

# The best company identifer that is share between all funds is StdCompanyName.
# Counter for mergedFundID starts from 1, increment each time accessed.
# Wipe all existing MergedFundIDs before starting.
# For each fund (row in MergedCharacteristics):
    # If StdCompanyName is blank, skip.
    # If fund already has a MergedFundID, skip.
    # Get all other rows with same StdCompanyName and no MergedFundID -> list F.
    # For each fund in set F:
        # Get all other rows with same StdCompanyName, same Currency and no MergedFundID -> list G.
        # Get all currencies in list G.
        # For each currency:
            # If size(G) = 1:
                # Record next MergedFundID 
            # Else:
                # List all pair combinations of members of G -> P.
                # For each pair (p1, p2) in P:
                    # Get the returns data for each (p1, p2) -> (r1, r2)
                    # Look at correlation of (r1, r2)
                    # If correlation >= 99%:
                        # Match! record next MergedFundID, same for both
                # For each unmatched pair:
                    # No match! record next MergedFundID 


