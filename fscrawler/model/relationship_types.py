from enum import Enum

# see: https://www.familysearch.org/developers/docs/api/types/json_FactType
# PARENT_CHILD_RELATIONSHIP_TYPES = [
#     "http://gedcomx.org/AdoptiveParent",        # A fact about an adoptive relationship
#     "http://gedcomx.org/BiologicalParent",      # A fact about the biological relationship
#     "http://gedcomx.org/FosterParent",          # A fact about a foster relationship
#     "http://gedcomx.org/GuardianParent",        # A fact about a legal guardianship
#     "http://gedcomx.org/StepParent",            # A fact about the step relationship
#     "http://gedcomx.org/SociologicalParent",    # A fact about a sociological relationship
#     "http://gedcomx.org/SurrogateParent",       # A fact about a pregnancy surrogate relationship
# ]


class RelationshipType(Enum):
    UNTYPED_PARENT = 'UntypedParent'                # We don't know, type not resolved, assumed to be Biological
    ASSUMED_BIOLOGICAL = 'AssumedBiological'        # We don't know, won't resolve, assumed to be Biological
    RESOLVE = 'Resolve'                             # Requires resolution
    UNSPECIFIED_PARENT = 'UnspecifiedParentType'    # We've requested type info, but didn't receive any
    UNTYPED_COUPLE = 'UntypedCouple'                # We don't know, type not resolved, assumed to be Marriage
    ADOPTIVE_PARENT = 'AdoptiveParent'
    BIOLOGICAL_PARENT = 'BiologicalParent'          # One of the FS API specified types (shortened)
    FOSTER_PARENT = 'FosterParent'
    GUARDIAN_PARENT = 'GuardianParent'
    STEP_PARENT = 'StepParent'
    SOCIOLOGICAL_PARENT = 'SociologicalParent'
    SURROGATE_PARENT = 'SurrogateParent'
