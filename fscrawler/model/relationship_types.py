# PARENT_CHILD_RELATIONSHIP_TYPES = [
#     "http://gedcomx.org/AdoptiveParent",        # A fact about an adoptive relationship between a parent and a child.
#     "http://gedcomx.org/BiologicalParent",      # A fact about the biological relationship between a parent and a child.
#     "http://gedcomx.org/FosterParent",          # A fact about a foster relationship between a foster parent and a child.
#     "http://gedcomx.org/GuardianParent",        # A fact about a legal guardianship between a parent and a child.
#     "http://gedcomx.org/StepParent",            # A fact about the step relationship between a parent and a child.
#     "http://gedcomx.org/SociologicalParent",    # A fact about a sociological relationship between a parent and a child, but not definable in typical legal or biological terms.
#     "http://gedcomx.org/SurrogateParent",       # A fact about a pregnancy surrogate relationship between a parent and a child.
# ]

BIOLOGICAL_PARENT = 'BiologicalParent'          # One of the FS API specified types (shortened)
UNTYPED_PARENT = 'UntypedParent'                # We don't know, we haven't requested type info yet, assumed to be Biological
UNSPECIFIED_PARENT = 'UnspecifiedParentType'    # We've requested type info, but didn't receive any
UNTYPED_COUPLE = 'UntypedCouple'                # We don't know, we haven't requested type info yet, assumed to be Marriage
