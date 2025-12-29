# Writing UDFs

Since Osprey UDFs are written in SML, you may find yourself wanting to write some rule logic that doesn't work very well - or simply isn't possible - with the existing functions
that are provided to you. For example, maybe you want to make an external API request, or perhaps you want to look up the DNS records for a given domain. Or, perhaps you have
some complex string parsing that you want to write that would be very complicated to implement.

This is where UDFs (user-defined functions) come into play. Every function that is available to you in the standard set of Osprey functions are also UDFs - things like `RegexMatch`,
`JsonData`, or `DomainChopper`. Let's go over the basics of writing a custom UDF that you can use inside of your own Osprey rules.

## Structuring Custom UDFs

Inside of the `example_plugins/src` directory, you'll find two things that we'll want to look at: the `udfs` directory and the `register_plugins.py` file. Custom UDFs that you
write do not _need_ to go inside of the `udfs` directory, but placing them there (or within subdirectories in that directory) helps to keep things organized. Once you have written
a UDF that you wish to use within your Osprey rules, you'll also need to "register" it within the `register_plugins.py` file.

```py
@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [
        MyCustomUdf,
    ]
```
]

## Basic UDFs

UDFs that you implement will generally extend the existing `UDFBase` class, found in `osprey.engine.udf.base`. This base UDF implements basic input arguments (`ArgumentsBase`) and
a `None` return type, and you may customize those to your choosing. For example, let's say that we want to write a simple UDF that checks if an input string contains a given phrase.
We'll need to accept two arguments: the haystack and the needle. Let's extend the `ArgumentsBase` class so we can get those arguments.

```py
class StringContainsArguments(ArgumentsBase):
    needle: str
    """The string to search for within the haystack"""
    haystack: str
    """The string to search for the needle within"""
```

Next, we'll implement the actual UDF. The code that runs when your Osprey rule calls a UDF lives inside the `execute` method of the `UDFBase` class. From within `execute`, you will
have access to both the arguments passed to the UDF as well as the `ExecutionContext`. This context provides you with the state of the event that triggered this UDF, for example through
`execution_context.get_data()`, `get_action_name()`, and `get_extracted_features()`. We won't need to worry about the `ExecutionContext` yet, but it's good to remember that it is there.

Let's go ahead and implement the `execute` method for this UDF we are making.

```py
# Create the UDF class, which extends UDFBase with the arguments class we created and the return ype of a bool
class StringContains(UDFBase[StringContainsArguments, bool]):
    # Define the execute method
    def execute(self, execution_context: ExecutionContext, arguments: StringContainsArguments):
        escaped = re.escape(arguments.needle)

        pattern = rf'{escaped}'

        regex = re.compile(pattern)

        # Return a bool, whether or not the needle was found within the haystack
        return bool(regex.search(arguments.haystack))
```

At it's simplest level, this does what we want: searches a given string for another specific string. If the string we are searching for is found, we return true and otherwise we return false.
We can now register this within `register_plugins.py` and use it within a SML rule.

```py
@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [
        MyCustomUdf,
        StringContains,
    ]
```

> [!NOTE]
> Function calls in SML must always use named arguments. For example, `StringContains("Sam", DisplayName)` is not valid. You must use `StringContains(needle="Sam", haystack=DisplayName)`.

```py
Import(
    rules=['models/base.sml'],
)

_NameContainsSam = StringContains(needle="Sam", haystack=DisplayName

NameHasSamRule = Rule(
    when_all=[_NameContainsSam],
    description='Name contains Sam!',
)

WhenRules(
    rules_any=[NameHasSamRule],
    then=[Label(entity=UserId, label='is-sam')],
)
```

## Optional Arguments and Default Values

After we deployed our new rule, we quickly discovered a problem. Not only were people who were _definitely_ named Sam getting labeled correctly, but there were also cases of people with names
like "Samantha" getting labeled as well! Some users also quickly discovered they could simply change their display name to a lowercased `sam` to avoid getting labeled correctly. We'll need to
modify our `StringContains` UDF to account for these issues.

Of course, we could simply modify the existing `StringContains` `execute` method to handle all of these cases with the needle, or we could create a whole new UDF like `StringContainsCaseInsensitive`,
but that isn't all that great. What we really want is to optionally tell the `StringContains` UDF to search for substrings and to ignore casing. To do that, we can add some optional arguments
to our `StringContainsArguments`.

```py
class StringContainsArguments(ArgumentsBase):
    needle: str
    """The string to search for within the haystack"""
    haystack: str
    """The string to search for the needle within"""

    case_insensitive: bool = False
    """Whether the needle should only be matched if the casing matches"""
    find_substrings: bool = False
    """Whether we want to find matches within substrings of the haystack, or if it needs to be an exact word"""
```

> [!NOTE]
> Similar to Pydantic models, you must always supply _some_ default value if you do not want to have to supply the parameter when calling the UDF, even for arguments that are typed as `Optional`.
> For example, if we used `case_insensitive: Optional[bool]`, we would get an error if we did not supply `case_insensitive` when calling the UDF. We would instead need to define it as
> `case_insensitive: Optional[bool] = None`.

Now that we have these arguments, we can modify our `execute` method to take them into consideration.

```py
class StringContains(UDFBase[StringContainsArguments, bool]):
    def execute(self, execution_context: ExecutionContext, arguments: StringContainsArguments):
        escaped = re.escape(arguments.needle)

        # Decide if we want to search for substrings or not...
        if arguments.substrings:
            pattern = rf'{escaped}'
        else:
            # If we do, we'll use word boundaries
            pattern = rf'\b{escaped}\b'

        # Define the flags for the regex search for case sensitivity
        flags = re.IGNORECASE if arguments.case_insensitive else 0

        regex = re.compile(pattern, flags)

        return bool(regex.search(arguments.haystack))
```

And now, we can update our Osprey rule to use these newly created arguments:

```py
Import(
    rules=['models/base.sml'],
)

_NameContainsSam = StringContains(
    needle="Sam",
    haystack=DisplayName,
    case_insensitive=True,
    # We do not need to supply the find_substrings argument since it was given a default value!
)

NameHasSamRule = Rule(
    when_all=[_NameContainsSam],
    description='Name contains Sam!',
)

WhenRules(
    rules_any=[NameHasSamRule],
    then=[Label(entity=UserId, label='is-sam')],
)
```

## Async Execution

For the majority of use cases, letting your UDFs execute synchronously is the way to go. However, there will be occasions where you instead for example want to write a rule that performs some sort of
network request. There is, of course, no sense letting that network request block the rest of your UDFs from executing. In those cases, we can use the `execute_async` class variable to tell the
Osprey engine that the given UDF should be - if possible - executed asynchronouosly from the rest of the UDF executions.

> [!NOTE]
> Setting `execute_async` to `True` is _not_ a guarantee that it will be ran asynchronously, but rather an indicator that it is a good candidate for asynchronous execution.

```py
class ScanImage(UDFBase[ScanImageArguments, Optional[float]]):
    execute_async = True

    def execute(self, execution_context: ExecutionContext, arguments: ScanImageArguments) -> Optional[float]:
        result = SomeLongRunningRequest(arguments.image_url)

        return result
```

## Query UDFs

Query UDFs are a special type of UDF that is callable from within the Osprey UI. You may find yourself in situations sometimes where you want to write a query UDF that lets you query
data in a more complicated way than is normally allowed with the basic SML query language.
