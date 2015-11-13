Release Notes
=======================

All #xxx numbers are the github issues.  All releases 0.14 and before were added retroactively, so appologies for anything that isn't quite right.

0.21 -- 2015-11-13 8:45
-----------------------------
* [BUG] #148 pymongo 3 compatability. pymongo 2.x should still work


0.20 -- 2015-11-10 7:32
-----------------------------
* [BUG] #150 Fix a bug when using _id=True to override mongo_id

0.19 -- 2015-01-17 12:14
-----------------------------
* [BUG] BadResultException has been moved into exceptions.py
* [BUG] Created and modified fields support all ComputedField kwargs
* [BUG] Indexes can now be created using the field instead of a string
* [BUG] Modified field always returned the current time

0.18 -- 2014-09-17 7:52
-----------------------------
* [FEATURE] Deprecate insert and add an identical method called save to session

0.17 -- 2014-09-16 7:50
-----------------------------
* [FEATURE] #132 Add .regex to QueryField ($regex), also .startswith and .endswith shortcuts (thanks dcollien!)

0.16 -- 2014-09-15 8:34
-----------------------------
* [BUG] #130 Bad error handling for invalid upserts


0.15 -- 2014-03-18 8:07
-----------------------------
* [FEATURE] Source-level support for python 3


0.14.4 -- 2014-03-04 8:07
-----------------------------
* [FEATURE] TTL indexes. Thanks to samyarous for the contribution!

0.14.3 -- 2013-11-28 10:43
-----------------------------
* [BUG] The elem match projection being used caused that field to be inaccessible on the object returned from the database


0.14 -- 2013-02-25 00:04
-----------------------------
* [FEATURE] #108: Add default_f, to allow a function to be used for default values
* [FEATURE] #107: Add normal pymongo sorting to query.sort and a new config, config_default_sort
* [FEATURE] #99: Schema migration via transform_incoming. This is a hook which allows schema migrations by allowing transforms of a document before MA touches it
* [BUG] #91: Saving a partial document raises FieldNotRetrieved

0.13.3 -- 2012-10-02 12:12
-----------------------------

* 0.13.2 and 0.13.1 had build issues
* [BREAKING] [FEATURE] #95: Check integrity on setting attributes, not on save
* [BREAKING] [FEATURE] #88: Support for transactions. The session with statement now opens and closes sessions.  Also added a caching mechanism.
* [FEATURE] #85: Timezone support. Pass a timezone into the session to enable. Times are always saved as UTC in the database.
* [BREAKING] The timezone feature also adds the requirement that unwrap functions on custom types to need take a session object (because the current session timezone needs to be accessible)
* [FEATURE] #28: Delegate to subclass. This allows subclasses with the same collection name
* [FEATURE] #7: Support for DBRefs. Check out RefField and SRefField in the fields documentation


0.12.2 -- 2012-09-10 01:32
-----------------------------
* [FEATURE] #92: Support for $exists. Thanks silversupreme!


0.12.1 -- 2012-05-20 18:13
-----------------------------
* [BUG] Use FloatField in GeoField


0.12 -- 2012-05-10 12:46
-----------------------------
* [FEATURE] geo index support


0.11.1 -- 2011-12-08 15:00
-----------------------------

* [BUG] Use pymongo 2.2 package re-ordering
