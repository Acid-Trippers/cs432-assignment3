from fastapi import APIRouter, HTTPException


router = APIRouter(prefix="/api/acid")


@router.get("/all")
async def run_all_acid_tests():
    try:
        from ACID.runner import run_all_tests

        return run_all_tests()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/advanced/{test_name}")
async def run_single_advanced_test(test_name: str):
    try:
        from ACID.runner import run_advanced_test

        return run_advanced_test(test_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{test_name}")
async def run_single_acid_test(test_name: str):
    try:
        from ACID.runner import run_acid_test

        return run_acid_test(test_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
