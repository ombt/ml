module TypeSignature exposing (Signature, parseSignature, showSignature, normalizeSignature, functionCompatibility, curry1, curry1Flip, sigIsArrow)

{-| This module provides the possibility to parse Haskell and Elm type signatures.
-}

import Combine as C exposing ((<$))
import Combine.Char as CC
import Combine.Num as CN
import Char
import Dict
import Tuple exposing (first)
import List exposing ((::))
import List.Extra exposing (permutations, subsequences)
import Maybe
import Result
import String


type Signature
    = Arrow Signature Signature
    | ListType Signature
      -- A Tuple with an empty List is the unit type.
    | Tuple (List Signature)
    | TypeConstructor String
    | TypeApplication Signature Signature
    | VariableType String


showSignature : Bool -> Signature -> String
showSignature charListAsString =
    showSignatureHelper charListAsString False False


splitLast : List a -> ( List a, a )
splitLast xs =
    case List.reverse xs of
        y :: ys ->
            ( List.reverse ys, y )

        _ ->
            "Error splitLast"
                |> Debug.crash


curry1 : Signature -> Signature
curry1 sig =
    case sig of
        Arrow (Tuple []) ret ->
            "Error curry1 (empty tuple): "
                ++ showSignature True sig
                |> Debug.crash

        Arrow (Tuple params) ret ->
            let
                ( ps, x ) =
                    splitLast params
            in
                case ps of
                    [p] -> Arrow p (Arrow x ret)
                    _ -> Arrow (Tuple ps) (Arrow x ret)

        Arrow sig ret ->
            Arrow (Tuple []) (Arrow sig ret)

        _ ->
            "Error curry1: "
                ++ showSignature True sig
                |> Debug.crash

curry1Flip : Signature -> Signature
curry1Flip sig =
    case (sig |> curry1) of
        Arrow a (Arrow b ret) ->
            Arrow b (Arrow a ret)
        _ ->
            "Error curry1Flip: "
                ++ showSignature True sig
                |> Debug.crash

mapS : (s -> String -> ( s, String )) -> s -> List Signature -> ( List Signature, s )
mapS f s =
    let
        go sig ( sigs, s ) =
            let
                ( sig_, s_ ) =
                    mapLRS f s sig
            in
                ( sig_ :: sigs, s_ )
    in
        List.foldl go ( [], s ) >> \( xs, s ) -> ( List.reverse xs, s )



{- http://stackoverflow.com/a/37455356/1866775 -}


mapLRS : (s -> String -> ( s, String )) -> s -> Signature -> ( Signature, s )
mapLRS f s sig =
    case sig of
        Arrow a b ->
            let
                ( a_, s_ ) =
                    mapLRS f s a

                ( b_, s__ ) =
                    mapLRS f s_ b
            in
                ( Arrow a_ b_, s__ )

        TypeConstructor x ->
            ( TypeConstructor x, s )

        VariableType x ->
            let
                ( s_, x_ ) =
                    f s x
            in
                ( VariableType x_, s_ )

        TypeApplication a b ->
            let
                ( a_, s_ ) =
                    mapLRS f s a

                ( b_, s__ ) =
                    mapLRS f s_ b
            in
                ( TypeApplication a_ b_, s__ )

        ListType x ->
            let
                ( x_, s_ ) =
                    mapLRS f s x
            in
                ( ListType x_, s_ )

        Tuple xs ->
            let
                ( xs_, s_ ) =
                    mapS f s xs
            in
                ( Tuple xs_, s_ )


nthVarName : Int -> String
nthVarName i =
    let
        charPart =
            97 + (rem i 26) |> Char.fromCode |> String.fromChar

        addNumber =
            i // 26

        numStr =
            if addNumber == 0 then
                ""
            else
                toString addNumber
    in
        charPart ++ numStr



{- Asserts varNames being generated by the same functions. -}


nextFreeVarName : List String -> String
nextFreeVarName varNames =
    nthVarName (List.length varNames)


normalizeSignatureGo :
    Dict.Dict String String
    -> String
    -> ( Dict.Dict String String, String )
normalizeSignatureGo dict str =
    let
        nextFree =
            nextFreeVarName (Dict.keys dict)

        str_ =
            Dict.get str dict |> Maybe.withDefault nextFree
    in
        ( Dict.insert str str_ dict, str_ )


normalizeSignature : Signature -> Signature
normalizeSignature =
    mapLRS normalizeSignatureGo Dict.empty >> first



--mapLRS (\s -> ( s + 1, s |> Char.fromCode |> String.fromChar )) 97 >> first


addParenthesis : String -> String
addParenthesis x =
    "(" ++ x ++ ")"


showSignatureHelper : Bool -> Bool -> Bool -> Signature -> String
showSignatureHelper charListAsString arrowsInParens typeAppInParens sig =
    let
        optArrowParens =
            if arrowsInParens then
                addParenthesis
            else
                identity

        optTypeApplicationParens =
            if typeAppInParens then
                addParenthesis
            else
                identity
    in
        case sig of
            Arrow a b ->
                showSignatureHelper charListAsString True False a
                    ++ " -> "
                    ++ showSignatureHelper charListAsString False False b
                    |> optArrowParens

            TypeConstructor x ->
                x

            VariableType x ->
                x

            TypeApplication a b ->
                showSignatureHelper charListAsString False False a
                    ++ " "
                    ++ showSignatureHelper charListAsString True True b
                    |> optTypeApplicationParens

            ListType (TypeConstructor "Char") ->
                if charListAsString then
                    "String"
                else
                    "[Char]"

            ListType x ->
                "[" ++ showSignatureHelper charListAsString False False x ++ "]"

            Tuple xs ->
                String.join ", "
                    (List.map (showSignatureHelper charListAsString False False) xs)
                    |> addParenthesis


listParser : C.Parser s Signature
listParser =
    C.brackets (C.lazy <| \() -> signatureParser)
        |> C.map ListType


trimSpaces : C.Parser s a -> C.Parser s a
trimSpaces =
    let
        skipSpaces =
            C.skipMany <| C.choice [ CC.space, CC.tab ]
    in
        C.between skipSpaces skipSpaces


tupleParser : C.Parser s Signature
tupleParser =
    let
        innerParser =
            C.sepBy (trimSpaces <| CC.char ',')
                (C.lazy <| \() -> signatureParser)
                |> C.map simplify

        simplify xs =
            case xs of
                [ x ] ->
                    x

                _ ->
                    Tuple xs
    in
        trimSpaces innerParser
            |> C.parens


arrowParser : C.Parser s Signature
arrowParser =
    let
        arrowOp =
            Arrow <$ trimSpaces (C.string "->")
    in
        C.chainr arrowOp (C.lazy <| \() -> nonAppSignatureParser)


isValidTypeApplication : Signature -> Bool
isValidTypeApplication sig =
    case sig of
        TypeConstructor _ ->
            True

        TypeApplication a b ->
            isValidTypeApplication a

        _ ->
            False


typeApplicationParser : C.Parser s Signature
typeApplicationParser =
    let
        typeApplyOp =
            TypeApplication <$ C.many1 CC.space

        validate ta =
            if isValidTypeApplication ta then
                C.succeed ta
            else
                C.fail "invalid type application"
    in
        C.andThen validate
            (C.chainl typeApplyOp (C.lazy <| \() -> nonOpSignatureParser))



typeStartsWithParser : C.Parser s Char -> (String -> Signature) -> C.Parser s Signature
typeStartsWithParser p tagger =
    [ p |> C.map (\x -> [ x ])
    , C.many <| C.choice [ CC.lower, CC.upper, CC.char '.', CC.char '_', CC.digit ]
    ]
        |> C.sequence
        |> C.map List.concat
        |> C.map (String.fromList >> tagger)


variableTypeParser : C.Parser s Signature
variableTypeParser =
    typeStartsWithParser CC.lower VariableType


stringToListChar : Signature -> Signature
stringToListChar sig =
    case sig of
        TypeConstructor "String" ->
            ListType (TypeConstructor "Char")

        _ ->
            sig


fixedTypeParser : C.Parser s Signature
fixedTypeParser =
    typeStartsWithParser CC.upper TypeConstructor |> C.map stringToListChar


nonOpSignatureParser : C.Parser s Signature
nonOpSignatureParser =
    C.choice
        [ C.lazy <| \() -> listParser
        , C.lazy <| \() -> tupleParser
        , variableTypeParser
        , fixedTypeParser
        ]


nonAppSignatureParser : C.Parser s Signature
nonAppSignatureParser =
    C.choice
        [ C.lazy <| \() -> typeApplicationParser
        , C.lazy <| \() -> nonOpSignatureParser
        ]


signatureParser : C.Parser s Signature
signatureParser =
    C.choice
        [ C.lazy <| \() -> arrowParser
        , nonAppSignatureParser
        ]
        |> trimSpaces


parseSignature : String -> Maybe Signature
parseSignature inputData =
    case C.parse signatureParser inputData of
        Ok (state, { input }, result) ->
            if String.isEmpty input then
                Maybe.Just result
            else
                Maybe.Nothing

        Err (state, stream, errors) ->
            Maybe.Nothing


equalityToFloat : Float -> Float -> a -> a -> Float
equalityToFloat valueTrue valueFalse x y =
    if x == y then
        valueTrue
    else
        valueFalse


sigIsArrow : Signature -> Bool
sigIsArrow sig =
    case sig of
        Arrow _ _ ->
            True

        _ ->
            False


functionCompatibility : Signature -> Signature -> Float
functionCompatibility db query =
    case ( db, query ) of
        ( VariableType _, TypeConstructor _ ) ->
            0.95

        ( VariableType _, ListType _ ) ->
            0.8

        ( TypeApplication (TypeConstructor "Maybe") (VariableType x), VariableType y ) ->
            0.8 * equalityToFloat 1.0 0.0 x y

        ( TypeApplication (TypeConstructor "Maybe") (TypeConstructor x), TypeConstructor y ) ->
            0.8 * equalityToFloat 1.0 0.0 x y

        ( Arrow a b, Arrow x y ) ->
            functionCompatibility a x * functionCompatibility b y

        ( TypeConstructor x, TypeConstructor y ) ->
            equalityToFloat 1.0 0.0 x y

        ( VariableType x, VariableType y ) ->
            equalityToFloat 1.0 0.85 x y

        ( TypeApplication a b, TypeApplication x y ) ->
            functionCompatibility a x * functionCompatibility b y

        ( ListType a, ListType x ) ->
            functionCompatibility a x

        ( Tuple xs, Tuple ys ) ->
            if List.length xs > List.length ys then
                List.map
                    (\xs_ ->
                        List.map2 functionCompatibility xs_ ys
                            |> List.product
                            |> (\x ->
                                    x
                                        * toFloat (List.length ys)
                                        / toFloat (List.length xs)
                               )
                    )
                    (subsequences xs)
                    |> List.maximum
                    |> Maybe.withDefault 0
            else if List.length xs == List.length ys then
                List.map
                    (\ys_ ->
                        List.map2 functionCompatibility xs ys_
                            |> List.product
                    )
                    (permutations ys)
                    |> List.maximum
                    |> Maybe.withDefault 0
            else
                0

        ( Tuple xs, y ) ->
            List.map
                (\x ->
                    functionCompatibility x y
                        / toFloat (List.length xs)
                )
                xs
                |> List.maximum
                |> Maybe.withDefault 0

        _ ->
            0