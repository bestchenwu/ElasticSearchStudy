package common.model;

public enum LevelEnum {

    FIRST(1,2),
    SECOND(2,3),
    THIRD(3,Float.MAX_VALUE),
    BAD(Float.MIN_VALUE,1);

    private float begin = Float.MIN_VALUE;
    private float end = Float.MAX_VALUE;

    private LevelEnum(float begin,float end){
        this.begin = begin;
        this.end = end;
    }

    public static LevelEnum parseLevelEnum(float value){
        for(LevelEnum level:LevelEnum.values()){
            if(level.begin<value && level.end>value){
                return level;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        LevelEnum levelEnum = LevelEnum.parseLevelEnum(3);
    }
}


